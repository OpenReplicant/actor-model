package kernel

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// Placement decides which node should host a given actor.
//
// It is deliberately a pure function of the cluster view: given the same
// actors and node capacities it returns the same node, so two schedulers
// looking at the same state make the same decision. It does not mutate
// anything, start anything, or talk to the network — the caller applies the
// result (usually by claiming the ownership lease on the chosen node).
//
// This is the piece the framework previously lacked. With leases alone an
// actor had one owner, but the operator still chose where to start it. With
// placement, a cluster can answer "given these actors and these nodes, where
// does each belong?" and converge on an assignment.
type Placement interface {
	// Place returns the node that should host req, given the current view.
	// It returns an error when no node satisfies the request.
	Place(ctx context.Context, req PlacementRequest, view PlacementView) (string, error)
}

// PlacementRequest describes the actor being placed.
type PlacementRequest struct {
	// Actor is the actor id being placed.
	Actor string
	// Requires is a label selector: the chosen node must carry every
	// key/value pair. Empty means any node.
	Requires map[string]string
	// Pin names an exact node, overriding the policy. Used for operator
	// overrides and for "keep it where it already is".
	Pin string
}

// PlacementView is the cluster state a placement decision is made from: for
// each known node, how many actors it already hosts and what labels it carries.
type PlacementView interface {
	// Nodes returns the known node names.
	Nodes() []string
	// Load returns how many actors a node currently hosts.
	Load(node string) int
	// Labels returns a node's labels, or nil.
	Labels(node string) map[string]string
}

// newPlacementView builds a PlacementView from plain maps. Used by tests and by
// callers that already have the cluster state in hand.
func newPlacementView(load map[string]int, labels map[string]map[string]string) PlacementView {
	return &mapPlacementView{load: load, labels: labels}
}

// mapPlacementView is the map-backed PlacementView.
type mapPlacementView struct {
	load   map[string]int
	labels map[string]map[string]string
}

func (v *mapPlacementView) Nodes() []string {
	out := make([]string, 0, len(v.load))
	for n := range v.load {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (v *mapPlacementView) Load(node string) int { return v.load[node] }

func (v *mapPlacementView) Labels(node string) map[string]string { return v.labels[node] }

// LeastLoadedPlacement spreads actors across nodes, respecting label
// requirements and explicit pins. Ties break on node name so the decision is
// deterministic.
type LeastLoadedPlacement struct{}

// NewLeastLoadedPlacement returns the default placement policy.
func NewLeastLoadedPlacement() *LeastLoadedPlacement { return &LeastLoadedPlacement{} }

// Place implements Placement.
func (p *LeastLoadedPlacement) Place(_ context.Context, req PlacementRequest, view PlacementView) (string, error) {
	if view == nil {
		return "", fmt.Errorf("placement: no cluster view")
	}
	nodes := view.Nodes()
	if len(nodes) == 0 {
		return "", fmt.Errorf("placement: no nodes available for actor %q", req.Actor)
	}

	if req.Pin != "" {
		for _, n := range nodes {
			if n == req.Pin {
				return n, nil
			}
		}
		return "", fmt.Errorf("placement: pinned node %q for actor %q is not in the cluster", req.Pin, req.Actor)
	}

	var best string
	bestLoad := -1
	for _, n := range nodes { // Nodes() is sorted, so ties resolve by name
		if !satisfiesLabels(view.Labels(n), req.Requires) {
			continue
		}
		load := view.Load(n)
		if bestLoad == -1 || load < bestLoad {
			best, bestLoad = n, load
		}
	}
	if best == "" {
		return "", fmt.Errorf("placement: no node satisfies %v for actor %q", req.Requires, req.Actor)
	}
	return best, nil
}

// satisfiesLabels reports whether node labels contain every required pair.
func satisfiesLabels(labels, requires map[string]string) bool {
	if len(requires) == 0 {
		return true
	}
	for k, want := range requires {
		if labels[k] != want {
			return false
		}
	}
	return true
}

// LeasePlacementView is a PlacementView derived from live ownership leases plus
// a node label registry. It is how placement sees reality: nodes are the nodes
// that hold leases, load is how many leases each holds.
type LeasePlacementView struct {
	leases *LeaseStore
	labels map[string]map[string]string
}

// NewLeasePlacementView builds a view over the lease store. labels may be nil;
// node labels are supplied by the operator (there is no gossip protocol here).
func NewLeasePlacementView(leases *LeaseStore, labels map[string]map[string]string) *LeasePlacementView {
	return &LeasePlacementView{leases: leases, labels: labels}
}

// Nodes returns the distinct nodes currently holding any live lease.
func (v *LeasePlacementView) Nodes() []string {
	if v.leases == nil {
		return nil
	}
	list, err := v.leases.List(context.Background())
	if err != nil {
		return nil
	}
	seen := make(map[string]bool)
	var out []string
	now := time.Now()
	for _, l := range list {
		if !l.Held(now) || seen[l.Node] {
			continue
		}
		seen[l.Node] = true
		out = append(out, l.Node)
	}
	sort.Strings(out)
	return out
}

// Load returns how many live leases a node holds.
func (v *LeasePlacementView) Load(node string) int {
	if v.leases == nil {
		return 0
	}
	list, err := v.leases.List(context.Background())
	if err != nil {
		return 0
	}
	now := time.Now()
	n := 0
	for _, l := range list {
		if l.Node == node && l.Held(now) {
			n++
		}
	}
	return n
}

// Labels returns the configured labels for a node.
func (v *LeasePlacementView) Labels(node string) map[string]string { return v.labels[node] }
