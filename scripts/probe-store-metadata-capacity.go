//go:build ignore

// Local tmpfs capacity/reload probe; synthetic backend, no remote S3 requests.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	stores3 "github.com/kuasar-sandbox/accelerator/pkg/store/s3"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type stub struct{}

func (stub) Head(context.Context, string) (*stores3.ObjectMeta, error) {
	return nil, stores3.ErrNotFound
}
func (stub) Get(context.Context, string) ([]byte, *stores3.ObjectMeta, error) {
	return nil, nil, stores3.ErrNotFound
}
func (stub) Put(context.Context, string, []byte, stores3.PutOptions) (string, error) {
	return "test", nil
}
func (stub) List(context.Context, string, func(string) bool) error { return nil }
func (stub) Delete(context.Context, string) error                  { return nil }
func sample() map[string]uint64 {
	runtime.GC()
	debug.FreeOSMemory()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	v := map[string]uint64{"heap_alloc": m.HeapAlloc, "heap_inuse": m.HeapInuse, "sys": m.Sys}
	b, _ := os.ReadFile("/proc/self/status")
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) > 1 && (f[0] == "VmRSS:" || f[0] == "VmHWM:") {
			n, _ := strconv.ParseUint(f[1], 10, 64)
			v[strings.TrimSuffix(f[0], ":")] = n * 1024
		}
	}
	return v
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	dir := flag.String("dir", "", "tmpfs cache root")
	mode := flag.String("mode", "reload", "fill or reload")
	count := flag.Int("count", 200000, "records")
	lookups := flag.Int("lookups", 100000, "number of lookups")
	randomOrder := flag.Bool("random", false, "query shuffled keys, precomputed outside the measurement")
	seed := flag.Int64("seed", 42, "shuffle seed")
	workers := flag.Int("workers", 1, "concurrent lookup workers")
	refresh := flag.Bool("refresh", true, "enable access-triggered verification on the measured hit path")
	flag.Parse()
	if *workers < 1 {
		return fmt.Errorf("workers must be positive")
	}
	before := sample()
	begin := time.Now()
	cfg := stores3.Config{Bucket: "capacity", ChunkMetadataCache: &stores3.ChunkMetadataCacheConfig{Dir: *dir, Namespace: stores3.CacheNamespace("probe", "region", "capacity", "", true), MaxTTL: 168 * time.Hour, MaxEntries: *count}}
	if *refresh {
		cfg.ChunkMetadataCache.RefreshAhead = &stores3.ChunkRefreshConfig{MaxQPS: 2}
	}
	s, err := stores3.New(context.Background(), stub{}, cfg)
	if err != nil {
		return err
	}
	defer s.Close()
	s.UpdateGenerations([]store.Generation{"G1"})
	reloadMS := float64(time.Since(begin).Microseconds()) / 1000
	if *mode == "fill" {
		for i := 0; i < *count; i++ {
			var data [8]byte
			binary.LittleEndian.PutUint64(data[:], uint64(i))
			key := store.ContentKey(sha256.Sum256(data[:]))
			h, e := s.OpenPut("G1", store.PartitionChunk, key, nil)
			if e != nil {
				return e
			}
			if _, e = h.Write(data[:]); e != nil {
				return e
			}
			if _, e = h.Commit(key, key); e != nil {
				return e
			}
		}
	}
	after := sample()
	stats := s.MetadataCacheStats()
	// Generate content keys and shuffle outside the measured window. The
	// production object-ID hash, validation, shard lock, visited flag and
	// refresh eligibility remain in the timed Exists call; hits do not query bbolt.
	if *count <= 0 || *lookups <= 0 {
		return fmt.Errorf("count and lookups must be positive")
	}
	keys := make([]store.ContentKey, *count)
	for i := range keys {
		var data [8]byte
		binary.LittleEndian.PutUint64(data[:], uint64(i))
		keys[i] = store.ContentKey(sha256.Sum256(data[:]))
	}
	order := make([]int, *count)
	for i := range order {
		order[i] = i
	}
	if *randomOrder {
		rand.New(rand.NewSource(*seed)).Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	}
	latencies := make([]int64, *lookups)
	runtime.GC()
	var cpuBefore, cpuAfter syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &cpuBefore); err != nil {
		return err
	}
	headsBefore, putsBefore := s.ChunkIOStats()
	lookupBegin := time.Now()
	expected := int64(8)
	var wg sync.WaitGroup
	errs := make(chan error, *workers)
	for worker := 0; worker < *workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < *lookups; i += *workers {
				key := keys[order[i%len(order)]]
				opBegin := time.Now()
				hit, e := s.Exists(context.Background(), "G1", store.PartitionChunk, key, &expected)
				latencies[i] = time.Since(opBegin).Nanoseconds()
				if e != nil {
					errs <- e
					return
				}
				if !hit {
					errs <- fmt.Errorf("unexpected miss %d", i)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		return e
	}
	lookupElapsed := time.Since(lookupBegin)
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &cpuAfter); err != nil {
		return err
	}
	userUS := (cpuAfter.Utime.Sec-cpuBefore.Utime.Sec)*1000000 + (cpuAfter.Utime.Usec - cpuBefore.Utime.Usec)
	sysUS := (cpuAfter.Stime.Sec-cpuBefore.Stime.Sec)*1000000 + (cpuAfter.Stime.Usec - cpuBefore.Stime.Usec)
	headsAfter, putsAfter := s.ChunkIOStats()
	if headsAfter != headsBefore || putsAfter != putsBefore {
		return fmt.Errorf("unexpected remote calls during cache lookup")
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(p int) int64 { return latencies[(len(latencies)-1)*p/100] }
	lookupNS := lookupElapsed.Nanoseconds() / int64(*lookups)
	// du counts tmpfs data blocks, not kernel inode/dentry slab allocations.
	du, e := exec.Command("du", "-s", "-B1", *dir).Output()
	if e != nil {
		return e
	}
	files, directories := 0, 0
	var dbLogicalBytes, allocatedBytes int64
	err = filepath.Walk(*dir, func(_ string, info os.FileInfo, e error) error {
		if e != nil {
			return e
		}
		if info.IsDir() {
			directories++
		} else {
			files++
			if stat, ok := info.Sys().(*syscall.Stat_t); ok {
				allocatedBytes += stat.Blocks * 512
			}
			if info.Name() == "metadata.db" {
				dbLogicalBytes += info.Size()
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	out := map[string]any{"mode": *mode, "entries": stats.Entries, "reloaded": stats.ReloadedEntries, "reload_ms": reloadMS, "elapsed_ms": float64(time.Since(begin).Microseconds()) / 1000, "before": before, "after": after, "du": strings.TrimSpace(string(du)), "regular_files": files, "directories": directories, "db_logical_bytes": dbLogicalBytes, "allocated_file_bytes": allocatedBytes, "hot_lookup_mean_ns": lookupNS,
		"refresh_enabled": *refresh, "workers": *workers, "lookups": *lookups, "random_order": *randomOrder, "seed": *seed, "gomaxprocs": runtime.GOMAXPROCS(0),
		"lookup_wall_ms": float64(lookupElapsed.Microseconds()) / 1000, "cpu_user_ms": float64(userUS) / 1000, "cpu_system_ms": float64(sysUS) / 1000,
		"cpu_percent_one_core": 100 * float64(userUS+sysUS) / float64(lookupElapsed.Microseconds()),
		"p50_ns":               percentile(50), "p95_ns": percentile(95), "p99_ns": percentile(99), "max_ns": latencies[len(latencies)-1],
		"remote_heads_during_lookup": headsAfter - headsBefore, "remote_puts_during_lookup": putsAfter - putsBefore}
	b, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(b))
	return nil
}
