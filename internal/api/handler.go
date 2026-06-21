package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/blocstor/bloc-manager/internal/agent"
	"github.com/blocstor/bloc-manager/internal/drbd"
	"github.com/blocstor/bloc-manager/internal/store"
)

const (
	drbdPort    = 7789
	drbdWaitDur = 2 * time.Second
)

// Handler holds the dependencies for all HTTP handlers.
type Handler struct {
	store       *store.Store
	agentConfig *agent.Config
	log         *slog.Logger
	vg          string
}

// New returns a new Handler.
func New(s *store.Store, cfg *agent.Config, log *slog.Logger, vg string) *Handler {
	return &Handler{store: s, agentConfig: cfg, log: log, vg: vg}
}

// RegisterRoutes registers all REST routes on mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/volumes", h.volumeHandler)
	mux.HandleFunc("/volumes/", h.volumeSubHandler)
	mux.HandleFunc("/healthz", h.healthz)
}

// healthz returns 200 OK.
func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok")) //nolint:errcheck
}

// volumeHandler handles POST /volumes and GET /volumes.
func (h *Handler) volumeHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.createVolume(w, r)
	case http.MethodGet:
		h.listVolumes(w, r)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// volumeSubHandler dispatches /volumes/:id and its sub-paths.
func (h *Handler) volumeSubHandler(w http.ResponseWriter, r *http.Request) {
	// Strip leading "/volumes/"
	path := strings.TrimPrefix(r.URL.Path, "/volumes/")
	parts := strings.SplitN(path, "/", 2)

	id := parts[0]
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing volume id")
		return
	}

	if len(parts) == 1 {
		// /volumes/:id
		h.volumeByIDHandler(w, r, id)
		return
	}

	switch parts[1] {
	case "publish":
		h.publishHandler(w, r, id)
	case "resize":
		h.resizeHandler(w, r, id)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

// ---- volume CRUD ----

type createVolumeRequest struct {
	Name   string   `json:"name"`
	SizeMB int      `json:"size_mb"`
	Nodes  []string `json:"nodes"`
}

type volumeResponse struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Minor          int      `json:"minor"`
	SizeMB         int      `json:"size_mb"`
	Status         string   `json:"status"`
	AttachedTo     string   `json:"attached_to,omitempty"`
	AttachedDevice string   `json:"attached_device,omitempty"`
	Nodes          []string `json:"nodes"`
}

func volumeToResponse(v store.Volume, status string) volumeResponse {
	return volumeResponse{
		ID:             v.ID,
		Name:           v.Name,
		Minor:          v.Minor,
		SizeMB:         v.SizeMB,
		Status:         status,
		AttachedTo:     v.AttachedTo,
		AttachedDevice: v.AttachedDevice,
		Nodes:          v.Nodes,
	}
}

// agentHostIP extracts the host IP from an agent URL like "http://192.168.0.151:8080".
func (h *Handler) agentHostIP(agentName string) string {
	url, ok := h.agentConfig.Agents[agentName]
	if !ok {
		return agentName
	}
	addr := strings.TrimPrefix(url, "https://")
	addr = strings.TrimPrefix(addr, "http://")
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}

// nbdPortFromDevice parses the port from an attached_device value like "nbd:10022".
func nbdPortFromDevice(dev string) int {
	s := strings.TrimPrefix(dev, "nbd:")
	port := 0
	fmt.Sscanf(s, "%d", &port)
	return port
}

// pcieTargetFromDevice returns the virtio-blk target from "pcie:vdb".
func pcieTargetFromDevice(dev string) string {
	return strings.TrimPrefix(dev, "pcie:")
}

// vmNextTarget returns the next free virtio-blk target name ("vdb", "vdc", …)
// given the set of already-attached targets.
func vmNextTarget(used []string) string {
	inUse := make(map[string]bool, len(used))
	for _, t := range used {
		inUse[t] = true
	}
	for _, c := range "bcdefghijklmnopqrstuvwxyz" {
		if t := "vd" + string(c); !inUse[t] {
			return t
		}
	}
	return ""
}

func (h *Handler) createVolume(w http.ResponseWriter, r *http.Request) {
	var req createVolumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.SizeMB <= 0 {
		writeError(w, http.StatusBadRequest, "size_mb must be positive")
		return
	}
	if len(req.Nodes) == 0 {
		writeError(w, http.StatusBadRequest, "at least one node is required")
		return
	}

	ctx := r.Context()

	minor, err := h.store.AllocateMinor()
	if err != nil {
		h.log.Error("allocate minor", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to allocate minor number")
		return
	}

	id := newID()
	lvName := fmt.Sprintf("drbd-%s", id)

	// Build DRBD res nodes — derive addresses from agent config.
	resNodes, err := h.buildResNodes(req.Nodes, minor)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	resContent, err := drbd.RenderRes(drbd.ResData{
		Name:   req.Name,
		VG:     h.vg,
		LVName: lvName,
		Minor:  minor,
		Nodes:  resNodes,
	})
	if err != nil {
		h.log.Error("render res", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to render DRBD res file")
		return
	}

	// Step 1: create LVs and write res files on all nodes.
	for _, nodeName := range req.Nodes {
		client, err := h.clientFor(nodeName)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		if err := client.CreateLV(ctx, h.vg, lvName, req.SizeMB); err != nil {
			h.log.Error("create lv", "node", nodeName, "err", err)
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("create LV on %s: %v", nodeName, err))
			return
		}

		if err := client.WriteRes(ctx, req.Name, resContent); err != nil {
			h.log.Error("write res", "node", nodeName, "err", err)
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("write res on %s: %v", nodeName, err))
			return
		}
	}

	// Step 2: initialize DRBD metadata on all nodes (required for fresh devices).
	for _, nodeName := range req.Nodes {
		client, _ := h.clientFor(nodeName)
		if err := client.DRBDCreateMD(ctx, req.Name); err != nil {
			h.log.Error("drbd create-md", "node", nodeName, "err", err)
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("drbd create-md on %s: %v", nodeName, err))
			return
		}
	}

	// Step 3: bring DRBD up on all nodes; roll back already-up nodes on failure.
	var upNodes []string
	for _, nodeName := range req.Nodes {
		client, _ := h.clientFor(nodeName)
		if err := client.DRBDUp(ctx, req.Name); err != nil {
			h.log.Error("drbd up", "node", nodeName, "err", err)
			// Roll back: bring down nodes that succeeded.
			for _, upNode := range upNodes {
				c, _ := h.clientFor(upNode)
				if rerr := c.DRBDDown(ctx, req.Name); rerr != nil {
					h.log.Warn("rollback drbd down", "node", upNode, "err", rerr)
				}
			}
			// Clean up res files and LVs on all nodes.
			for _, n := range req.Nodes {
				c, _ := h.clientFor(n)
				if rerr := c.RemoveRes(ctx, req.Name); rerr != nil {
					h.log.Warn("rollback remove res", "node", n, "err", rerr)
				}
				if rerr := c.RemoveLV(ctx, h.vg, lvName); rerr != nil {
					h.log.Warn("rollback remove lv", "node", n, "err", rerr)
				}
			}
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("drbd up on %s: %v", nodeName, err))
			return
		}
		upNodes = append(upNodes, nodeName)
	}

	// Step 4: wait briefly for DRBD to settle.
	time.Sleep(drbdWaitDur)

	// Step 5: persist.
	vol := store.Volume{
		ID:         id,
		Name:       req.Name,
		Nodes:      req.Nodes,
		Minor:      minor,
		SizeMB:     req.SizeMB,
		AttachedTo: "",
	}
	if err := h.store.CreateVolume(vol); err != nil {
		h.log.Error("create volume in store", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to persist volume")
		return
	}

	writeJSON(w, http.StatusCreated, volumeToResponse(vol, "available"))
}

func (h *Handler) listVolumes(w http.ResponseWriter, r *http.Request) {
	vols, err := h.store.ListVolumes()
	if err != nil {
		h.log.Error("list volumes", "err", err)
		writeError(w, http.StatusInternalServerError, "failed to list volumes")
		return
	}

	resp := make([]volumeResponse, 0, len(vols))
	for _, v := range vols {
		status := "available"
		if v.AttachedTo != "" {
			status = "attached"
		}
		resp = append(resp, volumeToResponse(v, status))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) volumeByIDHandler(w http.ResponseWriter, r *http.Request, id string) {
	switch r.Method {
	case http.MethodGet:
		h.getVolume(w, r, id)
	case http.MethodDelete:
		h.deleteVolume(w, r, id)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *Handler) getVolume(w http.ResponseWriter, r *http.Request, id string) {
	v, err := h.store.GetVolume(id)
	if err != nil {
		h.log.Error("get volume", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get volume")
		return
	}
	if v == nil {
		writeError(w, http.StatusNotFound, "volume not found")
		return
	}

	status := "available"
	if v.AttachedTo != "" {
		status = "attached"
	}
	writeJSON(w, http.StatusOK, volumeToResponse(*v, status))
}

func (h *Handler) deleteVolume(w http.ResponseWriter, r *http.Request, id string) {
	v, err := h.store.GetVolume(id)
	if err != nil {
		h.log.Error("get volume for delete", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get volume")
		return
	}
	if v == nil {
		writeError(w, http.StatusNotFound, "volume not found")
		return
	}

	ctx := r.Context()
	lvName := fmt.Sprintf("drbd-%s", v.ID)

	for _, nodeName := range v.Nodes {
		client, err := h.clientFor(nodeName)
		if err != nil {
			h.log.Warn("no client for node during delete", "node", nodeName)
			continue
		}
		// Best-effort teardown; ignore individual errors.
		if err := client.DRBDSecondary(ctx, v.Name); err != nil {
			h.log.Warn("drbd secondary on delete", "node", nodeName, "err", err)
		}
	}

	for _, nodeName := range v.Nodes {
		client, err := h.clientFor(nodeName)
		if err != nil {
			continue
		}
		if err := client.DRBDDown(ctx, v.Name); err != nil {
			h.log.Warn("drbd down on delete", "node", nodeName, "err", err)
		}
	}

	for _, nodeName := range v.Nodes {
		client, err := h.clientFor(nodeName)
		if err != nil {
			continue
		}
		if err := client.RemoveRes(ctx, v.Name); err != nil {
			h.log.Warn("remove res on delete", "node", nodeName, "err", err)
		}
		if err := client.RemoveLV(ctx, h.vg, lvName); err != nil {
			h.log.Warn("remove lv on delete", "node", nodeName, "err", err)
		}
	}

	if err := h.store.DeleteVolume(id); err != nil {
		h.log.Error("delete volume from store", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to delete volume from store")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ---- publish ----

type publishRequest struct {
	Node string `json:"node"`
}

type publishResponse struct {
	Node    string `json:"node"`
	Device  string `json:"device"`
	NBDHost string `json:"nbd_host"`
	NBDPort int    `json:"nbd_port"`
}

func (h *Handler) publishHandler(w http.ResponseWriter, r *http.Request, id string) {
	switch r.Method {
	case http.MethodPost:
		h.publishVolume(w, r, id)
	case http.MethodDelete:
		h.unpublishVolume(w, r, id)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (h *Handler) publishVolume(w http.ResponseWriter, r *http.Request, id string) {
	var req publishRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Node == "" {
		writeError(w, http.StatusBadRequest, "node is required")
		return
	}

	v, err := h.store.GetVolume(id)
	if err != nil {
		h.log.Error("get volume for publish", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get volume")
		return
	}
	if v == nil {
		writeError(w, http.StatusNotFound, "volume not found")
		return
	}

	// Idempotent: if already published to the same node, check if connection is active.
	if v.AttachedTo == req.Node {
		if strings.HasPrefix(v.AttachedDevice, "nbd:") {
			port := nbdPortFromDevice(v.AttachedDevice)
			vmInfo, err := h.resolveVMInfo(r.Context(), req.Node)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			hostIP := h.agentHostIP(vmInfo.Host)
			// Check if NBD server is active and listening on the current host.
			addr := fmt.Sprintf("%s:%d", hostIP, port)
			conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
			if err == nil {
				conn.Close()
				h.log.Info("NBD server is active, returning cached publish info", "addr", addr)
				writeJSON(w, http.StatusOK, publishResponse{
					Node:    req.Node,
					Device:  "/dev/nbd0",
					NBDHost: hostIP,
					NBDPort: port,
				})
				return
			}
			h.log.Info("NBD server not active or host changed, proceeding to start it", "addr", addr, "err", err)
		}
		if strings.HasPrefix(v.AttachedDevice, "pcie:") {
			target := pcieTargetFromDevice(v.AttachedDevice)
			vmInfo, err := h.resolveVMInfo(r.Context(), req.Node)
			if err == nil {
				if kvmClient, err := h.clientFor(vmInfo.Host); err == nil {
					if used, err := kvmClient.VMBlockList(r.Context(), vmInfo.Domain); err == nil {
						// Check if target is in the list of attached devices.
						attached := false
						for _, u := range used {
							if u == target {
								attached = true
								break
							}
						}
						if attached {
							h.log.Info("PCIe device is already attached to VM", "domain", vmInfo.Domain, "target", target)
							writeJSON(w, http.StatusOK, publishResponse{
								Node:   req.Node,
								Device: "/dev/" + target,
							})
							return
						}
					}
				}
			}
			h.log.Info("PCIe device not attached or error querying VM, proceeding to attach it", "target", target)
		}
	}

	// Resolve the KVM host for this Kubernetes node.
	vmInfo, err := h.resolveVMInfo(r.Context(), req.Node)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	kvmClient, err := h.clientFor(vmInfo.Host)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("unknown kvm host %q for node %q", vmInfo.Host, req.Node))
		return
	}

	ctx := r.Context()
	drbdDevice := fmt.Sprintf("/dev/drbd%d", v.Minor)

	// Demote the volume on all other replication hosts first to avoid "Multiple primaries not allowed by config".
	for _, nodeHost := range v.Nodes {
		if nodeHost == vmInfo.Host {
			continue
		}
		otherClient, err := h.clientFor(nodeHost)
		if err != nil {
			continue
		}
		h.log.Info("demoting volume on other host before promoting", "volume", v.Name, "otherHost", nodeHost)
		// Stop NBD if applicable. Always derive the port from v.Minor to clean up stale qemu-nbd servers,
		// since v.AttachedDevice is cleared on a previous unpublish call.
		port := 10000 + v.Minor
		if err := otherClient.NBDStop(ctx, port); err != nil {
			h.log.Warn("failed to stop NBD on other host", "volume", v.Name, "otherHost", nodeHost, "port", port, "err", err)
		} else {
			h.log.Info("stopped NBD on other host", "volume", v.Name, "otherHost", nodeHost, "port", port)
		}
		// Demote DRBD to Secondary.
		if err := otherClient.DRBDSecondary(ctx, v.Name); err != nil {
			h.log.Warn("failed to demote DRBD on other host", "volume", v.Name, "otherHost", nodeHost, "err", err)
		}
	}

	// Promote DRBD to Primary on the KVM host.
	if err := kvmClient.DRBDPrimaryForce(ctx, v.Name); err != nil {
		h.log.Error("drbd primary --force", "host", vmInfo.Host, "err", err)
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("promote DRBD on %s: %v", vmInfo.Host, err))
		return
	}

	attachMethod := vmInfo.AttachMethod
	if attachMethod == "" {
		attachMethod = "nbd"
	}

	switch attachMethod {
	case "pcie":
		used, err := kvmClient.VMBlockList(ctx, vmInfo.Domain)
		if err != nil {
			h.log.Error("vm block list", "host", vmInfo.Host, "domain", vmInfo.Domain, "err", err)
			_ = kvmClient.DRBDSecondary(ctx, v.Name)
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("list VM block devices on %s: %v", vmInfo.Host, err))
			return
		}

		target := vmNextTarget(used)
		if target == "" {
			_ = kvmClient.DRBDSecondary(ctx, v.Name)
			writeError(w, http.StatusInternalServerError, "no free virtio-blk target slots on VM")
			return
		}

		if err := kvmClient.VMAttach(ctx, vmInfo.Domain, drbdDevice, target); err != nil {
			h.log.Error("vm attach", "host", vmInfo.Host, "domain", vmInfo.Domain, "device", drbdDevice, "target", target, "err", err)
			_ = kvmClient.DRBDSecondary(ctx, v.Name)
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("attach device to VM on %s: %v", vmInfo.Host, err))
			return
		}

		v.AttachedTo = req.Node
		v.AttachedDevice = "pcie:" + target
		if err := h.store.UpdateVolume(*v); err != nil {
			h.log.Error("update volume attached_to", "id", id, "err", err)
			_ = kvmClient.VMDetach(ctx, vmInfo.Domain, target)
			_ = kvmClient.DRBDSecondary(ctx, v.Name)
			writeError(w, http.StatusInternalServerError, "failed to update volume")
			return
		}

		writeJSON(w, http.StatusOK, publishResponse{
			Node:   req.Node,
			Device: "/dev/" + target,
		})

	default: // "nbd"
		nbdPort := 10000 + v.Minor

		if err := kvmClient.NBDServe(ctx, drbdDevice, nbdPort); err != nil {
			h.log.Error("nbd serve", "host", vmInfo.Host, "device", drbdDevice, "port", nbdPort, "err", err)
			_ = kvmClient.DRBDSecondary(ctx, v.Name)
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("start NBD server on %s: %v", vmInfo.Host, err))
			return
		}

		hostIP := h.agentHostIP(vmInfo.Host)
		v.AttachedTo = req.Node
		v.AttachedDevice = fmt.Sprintf("nbd:%d", nbdPort)
		if err := h.store.UpdateVolume(*v); err != nil {
			h.log.Error("update volume attached_to", "id", id, "err", err)
			writeError(w, http.StatusInternalServerError, "failed to update volume")
			return
		}

		writeJSON(w, http.StatusOK, publishResponse{
			Node:    req.Node,
			Device:  "/dev/nbd0",
			NBDHost: hostIP,
			NBDPort: nbdPort,
		})
	}
}

func (h *Handler) unpublishVolume(w http.ResponseWriter, r *http.Request, id string) {
	v, err := h.store.GetVolume(id)
	if err != nil {
		h.log.Error("get volume for unpublish", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get volume")
		return
	}
	if v == nil {
		writeError(w, http.StatusNotFound, "volume not found")
		return
	}

	ctx := r.Context()

	// Detach the volume from its VM (NBD or PCIe).
	if v.AttachedTo != "" {
		if vmInfo, err := h.resolveVMInfo(ctx, v.AttachedTo); err == nil {
			if kvmClient, err := h.clientFor(vmInfo.Host); err == nil {
				switch {
				case strings.HasPrefix(v.AttachedDevice, "nbd:"):
					port := nbdPortFromDevice(v.AttachedDevice)
					if err := kvmClient.NBDStop(ctx, port); err != nil {
						h.log.Warn("nbd stop on unpublish", "host", vmInfo.Host, "port", port, "err", err)
					}
				case strings.HasPrefix(v.AttachedDevice, "pcie:"):
					target := pcieTargetFromDevice(v.AttachedDevice)
					if err := kvmClient.VMDetach(ctx, vmInfo.Domain, target); err != nil {
						h.log.Warn("vm detach on unpublish", "host", vmInfo.Host, "domain", vmInfo.Domain, "target", target, "err", err)
					}
				}
			}
		} else {
			h.log.Warn("unable to resolve VM info for unpublish", "node", v.AttachedTo, "err", err)
		}
	}

	for _, nodeName := range v.Nodes {
		client, err := h.clientFor(nodeName)
		if err != nil {
			h.log.Warn("no client for node during unpublish", "node", nodeName)
			continue
		}
		if err := client.DRBDSecondary(ctx, v.Name); err != nil {
			h.log.Warn("drbd secondary on unpublish", "node", nodeName, "err", err)
		}
	}

	v.AttachedTo = ""
	v.AttachedDevice = ""
	if err := h.store.UpdateVolume(*v); err != nil {
		h.log.Error("update volume for unpublish", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to update volume")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// ---- resize ----

type resizeRequest struct {
	NewSizeMB int `json:"new_size_mb"`
}

type resizeResponse struct {
	ID     string `json:"id"`
	SizeMB int    `json:"size_mb"`
}

func (h *Handler) resizeHandler(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req resizeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.NewSizeMB <= 0 {
		writeError(w, http.StatusBadRequest, "new_size_mb must be positive")
		return
	}

	v, err := h.store.GetVolume(id)
	if err != nil {
		h.log.Error("get volume for resize", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to get volume")
		return
	}
	if v == nil {
		writeError(w, http.StatusNotFound, "volume not found")
		return
	}

	if req.NewSizeMB <= v.SizeMB {
		writeError(w, http.StatusBadRequest, "new_size_mb must be larger than current size")
		return
	}

	addMB := req.NewSizeMB - v.SizeMB
	lvName := fmt.Sprintf("drbd-%s", v.ID)
	ctx := r.Context()

	// Extend LV on all nodes.
	for _, nodeName := range v.Nodes {
		client, err := h.clientFor(nodeName)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := client.ExtendLV(ctx, h.vg, lvName, addMB); err != nil {
			h.log.Error("extend lv", "node", nodeName, "err", err)
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("extend LV on %s: %v", nodeName, err))
			return
		}
	}

	// Resize DRBD on the primary node, if attached.
	if v.AttachedTo != "" {
		client, err := h.clientFor(v.AttachedTo)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		if err := client.DRBDResize(ctx, v.Name); err != nil {
			h.log.Error("drbd resize", "node", v.AttachedTo, "err", err)
			writeError(w, http.StatusInternalServerError, fmt.Sprintf("drbd resize on %s: %v", v.AttachedTo, err))
			return
		}
	}

	v.SizeMB = req.NewSizeMB
	if err := h.store.UpdateVolume(*v); err != nil {
		h.log.Error("update volume size", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "failed to update volume size")
		return
	}

	writeJSON(w, http.StatusOK, resizeResponse{ID: v.ID, SizeMB: v.SizeMB})
}

// ---- helpers ----

// newID returns a random 8-byte hex string suitable for use as a volume ID.
func newID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}

// clientFor returns an agent.Client for the named node, or an error if
// the node is not in the configuration.
func (h *Handler) clientFor(nodeName string) (*agent.Client, error) {
	url, ok := h.agentConfig.Agents[nodeName]
	if !ok {
		return nil, fmt.Errorf("unknown node %q", nodeName)
	}
	return agent.NewClient(url), nil
}

// buildResNodes converts node names to drbd.ResNode entries using agent config
// to derive the host address (stripping the http(s):// scheme and port).
// minor is added to drbdPort so each resource uses a unique port.
func (h *Handler) buildResNodes(nodes []string, minor int) ([]drbd.ResNode, error) {
	result := make([]drbd.ResNode, 0, len(nodes))
	for _, name := range nodes {
		url, ok := h.agentConfig.Agents[name]
		if !ok {
			return nil, fmt.Errorf("unknown node %q", name)
		}
		// Strip scheme.
		addr := strings.TrimPrefix(url, "https://")
		addr = strings.TrimPrefix(addr, "http://")
		// Strip port from base URL (agent port), keep only host.
		if idx := strings.LastIndex(addr, ":"); idx != -1 {
			addr = addr[:idx]
		}
		result = append(result, drbd.ResNode{
			Hostname: name,
			Address:  addr,
			Port:     drbdPort + minor,
		})
	}
	return result, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write json response", "err", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// resolveVMInfo dynamically locates the host for a given VM node name by querying all agents.
func (h *Handler) resolveVMInfo(ctx context.Context, nodeName string) (*agent.VMInfo, error) {
	// First, check if it's in the static config.
	if vmInfo, ok := h.agentConfig.VMs[nodeName]; ok {
		// Verify if the VM is actually running on the configured host.
		client, err := h.clientFor(vmInfo.Host)
		if err == nil {
			if _, err := client.VMBlockList(ctx, vmInfo.Domain); err == nil {
				return &vmInfo, nil
			}
		}
	}

	// Probing all agents dynamically if it's not found or not active on the configured host.
	for hostName := range h.agentConfig.Agents {
		client, err := h.clientFor(hostName)
		if err != nil {
			continue
		}
		// In our platform, the domain name of the VM matches the Kubernetes node name (e.g. cluster-a-worker-0).
		if _, err := client.VMBlockList(ctx, nodeName); err == nil {
			attachMethod := "nbd"
			if vmInfo, ok := h.agentConfig.VMs[nodeName]; ok && vmInfo.AttachMethod != "" {
				attachMethod = vmInfo.AttachMethod
			}
			return &agent.VMInfo{
				Host:         hostName,
				Domain:       nodeName,
				AttachMethod: attachMethod,
			}, nil
		}
	}

	return nil, fmt.Errorf("VM %q not found or not running on any KVM host", nodeName)
}

