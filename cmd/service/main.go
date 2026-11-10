// Command service is a deliberately fragile microservice.
// It exposes /health and /work, plus /chaos/<mode> endpoints that let us
// break it on purpose (crash, hang, slow, errors) so the control plane has
// something to heal.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	name := flag.String("name", "service", "service name")
	port := flag.Int("port", 9100, "listen port")
	random := flag.Bool("random-chaos", false, "inject a random fault every 20-60s")
	flag.Parse()

	var mode atomic.Value // "ok", "hang", "slow" or "errors"
	mode.Store("ok")

	// behave applies the current fault. It returns true if it already
	// answered (or abandoned) the request.
	behave := func(w http.ResponseWriter) bool {
		switch mode.Load().(string) {
		case "hang":
			time.Sleep(30 * time.Second)
			return true
		case "slow":
			time.Sleep(1500 * time.Millisecond)
		case "errors":
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return true
		}
		return false
	}
	reply := func(w http.ResponseWriter, extra string) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"service": *name, "port": *port, "mode": mode.Load(), "info": extra,
		})
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if !behave(w) {
			reply(w, "healthy")
		}
	})
	mux.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
		if behave(w) {
			return
		}
		time.Sleep(time.Duration(5+rand.Intn(20)) * time.Millisecond) // pretend to work
		reply(w, "work done")
	})
	mux.HandleFunc("/chaos/", func(w http.ResponseWriter, r *http.Request) {
		m := strings.TrimPrefix(r.URL.Path, "/chaos/")
		switch m {
		case "crash":
			reply(w, "crashing")
			go func() { time.Sleep(50 * time.Millisecond); os.Exit(1) }()
		case "heal":
			mode.Store("ok")
			reply(w, "healed")
		case "hang", "slow", "errors":
			mode.Store(m)
			reply(w, "fault injected: "+m)
		default:
			http.Error(w, "unknown chaos mode", http.StatusBadRequest)
		}
	})

	if *random {
		go func() {
			faults := []string{"hang", "slow", "errors", "crash"}
			for {
				time.Sleep(time.Duration(20+rand.Intn(40)) * time.Second)
				f := faults[rand.Intn(len(faults))]
				log.Printf("[%s] random chaos: %s", *name, f)
				if f == "crash" {
					os.Exit(1)
				}
				mode.Store(f)
			}
		}()
	}

	addr := fmt.Sprintf("127.0.0.1:%d", *port)
	log.Printf("[%s] listening on %s", *name, addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
