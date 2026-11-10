// Command controlplane is a mini Kubernetes-style control loop.
//
//	Prober      one goroutine per replica, checks /health every second
//	Reconciler  compares desired vs actual state and heals the difference
//	Supervisor  starts, kills and restarts service processes (with backoff)
//	Router      sends traffic only to healthy replicas, retrying on failure
//	Dashboard   web UI + JSON API on the same port
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed web/index.html
var indexHTML []byte

type State string

const (
	Starting   State = "starting"
	Healthy    State = "healthy"
	Suspect    State = "suspect"
	Unhealthy  State = "unhealthy"
	Restarting State = "restarting"
)

const (
	failThreshold = 3 // consecutive failed probes before "unhealthy"
	passThreshold = 2 // consecutive good probes before "healthy"
	probeTimeout  = 800 * time.Millisecond
	startGrace    = 5 * time.Second // failures are ignored while a process boots
)

type Replica struct {
	ID        string `json:"id"`
	Service   string `json:"service"`
	Port      int    `json:"port"`
	State     State  `json:"state"`
	Fails     int    `json:"fails"`
	Restarts  int    `json:"restarts"`
	LatencyMs int64  `json:"latencyMs"`
	Fault     string `json:"fault"`

	passes, streak int
	cmd            *exec.Cmd
	startedAt      time.Time
	faultAt        time.Time
	restarting     bool
	removed        bool
	ctx            context.Context
	cancel         context.CancelFunc
}

type Service struct {
	Name     string
	Desired  int
	Replicas []*Replica
	nextID   int
	rr       uint64
	ok, tot  int64
	history  []int // success % per second, last 60s
	lastOK   int64
	lastTot  int64
}

type Event struct {
	Time string `json:"time"`
	Kind string `json:"kind"` // ok | warn | bad | info
	Msg  string `json:"msg"`
}

type ControlPlane struct {
	mu       sync.RWMutex
	svcs     map[string]*Service
	order    []string
	nextPort int
	bin      string
	random   bool
	traffic  atomic.Bool
	started  time.Time
	mttd     []float64 // seconds from fault to "unhealthy"
	mttr     []float64 // seconds from fault to "healthy again"

	evMu   sync.Mutex
	events []Event
}

func (c *ControlPlane) logEvent(kind, msg string) {
	c.evMu.Lock()
	defer c.evMu.Unlock()
	c.events = append([]Event{{time.Now().Format("15:04:05"), kind, msg}}, c.events...)
	if len(c.events) > 80 {
		c.events = c.events[:80]
	}
	log.Printf("[%s] %s", kind, msg)
}

// ---------- Supervisor ----------

func (c *ControlPlane) newReplica(s *Service) *Replica {
	s.nextID++
	c.nextPort++
	r := &Replica{ID: fmt.Sprintf("%s-%d", s.Name, s.nextID), Service: s.Name, Port: c.nextPort, State: Starting}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	return r
}

// start launches the OS process for a replica.
func (c *ControlPlane) start(r *Replica) {
	args := []string{"-name", r.Service, "-port", strconv.Itoa(r.Port)}
	if c.random {
		args = append(args, "-random-chaos")
	}
	cmd := exec.Command(c.bin, args...)
	if err := cmd.Start(); err != nil {
		c.logEvent("bad", fmt.Sprintf("%s: cannot start process: %v", r.ID, err))
		return
	}
	c.mu.Lock()
	r.cmd, r.State, r.passes, r.Fails, r.Fault, r.startedAt = cmd, Starting, 0, 0, "", time.Now()
	c.mu.Unlock()
	c.logEvent("info", fmt.Sprintf("%s: started on port %d", r.ID, r.Port))
	go func() {
		cmd.Wait()
		c.onExit(r, cmd)
	}()
}

// onExit notices a process that died by itself (a crash).
func (c *ControlPlane) onExit(r *Replica, cmd *exec.Cmd) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.removed || r.cmd != cmd || r.State == Restarting || r.State == Unhealthy {
		return
	}
	c.markUnhealthy(r, "process exited")
}

// markUnhealthy removes a replica from routing. Caller must hold c.mu.
func (c *ControlPlane) markUnhealthy(r *Replica, why string) {
	r.State = Unhealthy
	if r.faultAt.IsZero() {
		r.faultAt = time.Now()
	} else {
		d := time.Since(r.faultAt).Seconds()
		c.mttd = append(c.mttd, d)
		why += fmt.Sprintf(", detected in %.1fs", d)
	}
	c.logEvent("bad", fmt.Sprintf("%s: %s, removed from routing", r.ID, why))
}

// restart kills the process and starts a new one after an exponential backoff.
func (c *ControlPlane) restart(r *Replica) {
	c.mu.Lock()
	r.streak++
	backoff := time.Duration(1<<uint(min(r.streak-1, 4))) * time.Second // 1,2,4,8,16s
	r.State, r.Restarts = Restarting, r.Restarts+1
	old := r.cmd
	c.mu.Unlock()
	c.logEvent("warn", fmt.Sprintf("%s: restarting in %s (attempt %d)", r.ID, backoff, r.streak))
	if old != nil && old.Process != nil {
		old.Process.Kill()
	}
	select {
	case <-time.After(backoff):
		c.start(r)
	case <-r.ctx.Done():
	}
	c.mu.Lock()
	r.restarting = false
	c.mu.Unlock()
}

// ---------- Prober ----------

func (c *ControlPlane) probe(r *Replica) {
	client := &http.Client{Timeout: probeTimeout}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-t.C:
			begin := time.Now()
			resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/health", r.Port))
			ok := err == nil && resp.StatusCode == http.StatusOK
			if resp != nil {
				resp.Body.Close()
			}
			c.onProbe(r, ok, time.Since(begin))
		}
	}
}

// onProbe is the health state machine:
// healthy -> suspect (1 miss) -> unhealthy (3 misses) -> restarting -> starting -> healthy.
func (c *ControlPlane) onProbe(r *Replica, ok bool, lat time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r.State == Restarting || r.State == Unhealthy {
		return
	}
	r.LatencyMs = lat.Milliseconds()
	if ok {
		r.Fails = 0
		r.passes++
		switch {
		case r.State == Suspect:
			r.State = Healthy
			c.logEvent("info", r.ID+": probe recovered, back to healthy")
		case r.State == Starting && r.passes >= passThreshold:
			r.State, r.streak = Healthy, 0
			if !r.faultAt.IsZero() {
				d := time.Since(r.faultAt).Seconds()
				c.mttr = append(c.mttr, d)
				c.logEvent("ok", fmt.Sprintf("%s: recovered, back in rotation (%.1fs after fault)", r.ID, d))
				r.faultAt = time.Time{}
			} else {
				c.logEvent("ok", r.ID+": healthy")
			}
		}
		return
	}
	r.passes = 0
	if r.State == Starting && time.Since(r.startedAt) < startGrace {
		return
	}
	r.Fails++
	if r.State == Healthy {
		r.State = Suspect
		c.logEvent("warn", fmt.Sprintf("%s: health probe failed (1/%d)", r.ID, failThreshold))
	}
	if r.Fails >= failThreshold {
		c.markUnhealthy(r, fmt.Sprintf("%d failed probes", failThreshold))
	}
}

// ---------- Reconciler ----------

// reconcile makes actual state match desired state. It runs every 500ms.
func (c *ControlPlane) reconcile() {
	var spawn, heal []*Replica
	c.mu.Lock()
	for _, name := range c.order {
		s := c.svcs[name]
		for len(s.Replicas) < s.Desired {
			r := c.newReplica(s)
			s.Replicas = append(s.Replicas, r)
			spawn = append(spawn, r)
			go c.probe(r)
		}
		for len(s.Replicas) > s.Desired {
			r := s.Replicas[len(s.Replicas)-1]
			s.Replicas = s.Replicas[:len(s.Replicas)-1]
			r.removed = true
			r.cancel()
			if r.cmd != nil && r.cmd.Process != nil {
				r.cmd.Process.Kill()
			}
			c.logEvent("info", r.ID+": scaled down")
		}
		for _, r := range s.Replicas {
			if r.State == Unhealthy && !r.restarting {
				r.restarting = true
				heal = append(heal, r)
			}
		}
	}
	c.mu.Unlock()
	for _, r := range spawn {
		c.start(r)
	}
	for _, r := range heal {
		go c.restart(r)
	}
}

// ---------- Router ----------

var fwd = &http.Client{Timeout: time.Second}

// route sends one request to a healthy replica, retrying once on another one.
func (c *ControlPlane) route(name, path string) (int, []byte) {
	c.mu.RLock()
	s := c.svcs[name]
	if s == nil {
		c.mu.RUnlock()
		return http.StatusNotFound, []byte("unknown service")
	}
	var pool []*Replica
	off := int(atomic.AddUint64(&s.rr, 1))
	for i := range s.Replicas {
		r := s.Replicas[(i+off)%len(s.Replicas)]
		if r.State == Healthy || r.State == Suspect {
			pool = append(pool, r)
		}
	}
	c.mu.RUnlock()

	code, body := http.StatusServiceUnavailable, []byte("no healthy replicas")
	for i := 0; i < len(pool) && i < 2; i++ {
		resp, err := fwd.Get(fmt.Sprintf("http://127.0.0.1:%d%s", pool[i].Port, path))
		if err != nil {
			code, body = http.StatusBadGateway, []byte(err.Error())
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		code, body = resp.StatusCode, b
		if code < 500 {
			break
		}
	}
	atomic.AddInt64(&s.tot, 1)
	if code < 500 {
		atomic.AddInt64(&s.ok, 1)
	}
	return code, body
}

func (c *ControlPlane) trafficLoop() {
	t := time.NewTicker(200 * time.Millisecond)
	for range t.C {
		if !c.traffic.Load() {
			continue
		}
		for _, name := range c.order {
			go c.route(name, "/work")
		}
	}
}

// sampleLoop records a success-rate sample per service every second (sparklines).
func (c *ControlPlane) sampleLoop() {
	for range time.Tick(time.Second) {
		c.mu.Lock()
		for _, s := range c.svcs {
			ok, tot := atomic.LoadInt64(&s.ok), atomic.LoadInt64(&s.tot)
			if d := tot - s.lastTot; d > 0 {
				s.history = append(s.history, int(100*(ok-s.lastOK)/d))
			} else if len(s.history) > 0 {
				s.history = append(s.history, s.history[len(s.history)-1])
			}
			if len(s.history) > 60 {
				s.history = s.history[1:]
			}
			s.lastOK, s.lastTot = ok, tot
		}
		c.mu.Unlock()
	}
}

// ---------- API ----------

func avg(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	t := 0.0
	for _, x := range v {
		t += x
	}
	return t / float64(len(v))
}

func (c *ControlPlane) state(w http.ResponseWriter, _ *http.Request) {
	type svcView struct {
		Name     string     `json:"name"`
		Desired  int        `json:"desired"`
		Replicas []*Replica `json:"replicas"`
		OK       int64      `json:"ok"`
		Total    int64      `json:"total"`
		History  []int      `json:"history"`
	}
	c.evMu.Lock()
	events := append([]Event(nil), c.events...)
	c.evMu.Unlock()

	c.mu.RLock()
	var out []svcView
	var okAll, totAll int64
	for _, name := range c.order {
		s := c.svcs[name]
		o, t := atomic.LoadInt64(&s.ok), atomic.LoadInt64(&s.tot)
		okAll, totAll = okAll+o, totAll+t
		out = append(out, svcView{name, s.Desired, s.Replicas, o, t, s.history})
	}
	// Marshal while holding the lock: replicas are mutated by other goroutines.
	data, _ := json.Marshal(map[string]any{
		"services": out, "events": events, "traffic": c.traffic.Load(),
		"uptime": int(time.Since(c.started).Seconds()), "okTotal": okAll, "total": totAll,
		"avgMttd": avg(c.mttd), "avgMttr": avg(c.mttr), "recoveries": len(c.mttr),
	})
	c.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (c *ControlPlane) findReplica(id string) *Replica {
	for _, s := range c.svcs {
		for _, r := range s.Replicas {
			if r.ID == id {
				return r
			}
		}
	}
	return nil
}

func (c *ControlPlane) chaos(w http.ResponseWriter, req *http.Request) {
	id, mode := req.FormValue("id"), req.FormValue("mode")
	c.mu.Lock()
	r := c.findReplica(id)
	if r == nil {
		c.mu.Unlock()
		http.Error(w, "unknown replica", http.StatusNotFound)
		return
	}
	port := r.Port
	if mode == "kill" {
		r.faultAt = time.Now()
		if r.cmd != nil && r.cmd.Process != nil {
			r.cmd.Process.Kill()
		}
	} else if mode != "heal" {
		if r.faultAt.IsZero() {
			r.faultAt = time.Now()
		}
		r.Fault = mode
	} else {
		r.Fault, r.faultAt = "", time.Time{}
	}
	c.mu.Unlock()
	c.logEvent("info", fmt.Sprintf("%s: chaos injected -> %s", id, mode))
	if mode != "kill" {
		if resp, err := fwd.Get(fmt.Sprintf("http://127.0.0.1:%d/chaos/%s", port, mode)); err == nil {
			resp.Body.Close()
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (c *ControlPlane) scale(w http.ResponseWriter, req *http.Request) {
	d, _ := strconv.Atoi(req.FormValue("delta"))
	c.mu.Lock()
	if s := c.svcs[req.FormValue("service")]; s != nil {
		s.Desired = max(0, min(5, s.Desired+d))
		c.logEvent("info", fmt.Sprintf("desired state: service %s -> %d replicas", s.Name, s.Desired))
	}
	c.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// ---------- main ----------

func ensureBinary(path string) {
	if _, err := os.Stat(path); err == nil {
		return
	}
	log.Println("building service binary ...")
	out, err := exec.Command("go", "build", "-o", path, "./cmd/service").CombinedOutput()
	if err != nil {
		log.Fatalf("could not build service: %v\n%s (run this from the project root)", err, out)
	}
}

func main() {
	addr := flag.String("addr", ":8080", "dashboard / API / proxy address")
	replicas := flag.Int("replicas", 2, "initial replicas per service")
	random := flag.Bool("random-chaos", false, "let services break themselves randomly")
	flag.Parse()

	bin := "bin/service"
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	ensureBinary(bin)

	c := &ControlPlane{svcs: map[string]*Service{}, nextPort: 9100, bin: bin, random: *random, started: time.Now()}
	for _, n := range []string{"a", "b", "c"} {
		c.svcs[n] = &Service{Name: n, Desired: *replicas}
		c.order = append(c.order, n)
	}
	c.logEvent("info", "control plane started")

	go func() { // the control loop
		for range time.Tick(500 * time.Millisecond) {
			c.reconcile()
		}
	}()
	go c.trafficLoop()
	go c.sampleLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	mux.HandleFunc("/api/state", c.state)
	mux.HandleFunc("/api/chaos", c.chaos)
	mux.HandleFunc("/api/scale", c.scale)
	mux.HandleFunc("/api/traffic", func(w http.ResponseWriter, r *http.Request) {
		on := r.FormValue("on") == "true"
		c.traffic.Store(on)
		c.logEvent("info", fmt.Sprintf("traffic generator %v", map[bool]string{true: "started", false: "stopped"}[on]))
		w.WriteHeader(http.StatusNoContent)
	})
	// Public entry point: /svc/a/work -> a healthy replica of service "a".
	mux.HandleFunc("/svc/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/svc/"), "/", 2)
		path := "/"
		if len(parts) == 2 {
			path += parts[1]
		}
		code, body := c.route(parts[0], path)
		w.WriteHeader(code)
		w.Write(body)
	})

	srv := &http.Server{Addr: *addr, Handler: mux}
	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt)
		<-stop
		log.Println("shutting down, stopping all services ...")
		c.mu.Lock()
		for _, s := range c.svcs {
			for _, r := range s.Replicas {
				r.removed = true
				r.cancel()
				if r.cmd != nil && r.cmd.Process != nil {
					r.cmd.Process.Kill()
				}
			}
		}
		c.mu.Unlock()
		srv.Close()
	}()
	log.Printf("dashboard: http://localhost%s", *addr)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
