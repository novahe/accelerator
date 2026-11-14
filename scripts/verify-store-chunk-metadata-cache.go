//go:build ignore

// Standalone integration probe. Runs a real MinIO and store-ctl as child
// processes, counts S3 HTTP requests through a loopback proxy, and joins them.
// Run from the module root with go run -buildvcs=false ./scripts/verify-store-chunk-metadata-cache.go.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/kuasar-sandbox/accelerator/pkg/store"
	storeclient "github.com/kuasar-sandbox/accelerator/pkg/store/client"
	stores3 "github.com/kuasar-sandbox/accelerator/pkg/store/s3"
)

type counts struct {
	Heads int `json:"heads"`
	Puts  int `json:"puts"`
	Gets  int `json:"gets"`
}

func (a counts) sub(b counts) counts {
	return counts{a.Heads - b.Heads, a.Puts - b.Puts, a.Gets - b.Gets}
}

type result struct {
	Name      string          `json:"name"`
	Calls     counts          `json:"calls"`
	ElapsedMS float64         `json:"elapsed_ms"`
	Snapshot  json.RawMessage `json:"snapshot"`
}
type child struct {
	cmd     *exec.Cmd
	done    chan error
	log     *os.File
	stopped bool
}

func start(binary string, args []string, logfile string, env []string) (*child, error) {
	log, err := os.Create(logfile)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(binary, args...)
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.Env = append(os.Environ(), env...)
	if err = cmd.Start(); err != nil {
		log.Close()
		return nil, err
	}
	c := &child{cmd: cmd, done: make(chan error, 1), log: log}
	go func() { c.done <- cmd.Wait() }()
	return c, nil
}
func (c *child) stop(crash bool) error {
	if c == nil || c.stopped {
		return nil
	}
	c.stopped = true
	if crash {
		_ = c.cmd.Process.Kill()
	} else {
		_ = c.cmd.Process.Signal(syscall.SIGTERM)
	}
	var err error
	select {
	case err = <-c.done:
	case <-time.After(15 * time.Second):
		_ = c.cmd.Process.Kill()
		err = <-c.done
	}
	_ = c.log.Close()
	if crash {
		return nil
	}
	return err
}
func address() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	a := l.Addr().String()
	_ = l.Close()
	return a, nil
}
func save(name string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(name, append(b, '\n'), 0o600)
}
func require(ok bool, format string, args ...any) error {
	if !ok {
		return fmt.Errorf(format, args...)
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	storeBin := flag.String("store-bin", "", "built store-ctl path")
	minioBin := flag.String("minio-bin", "", "MinIO executable path")
	out := flag.String("out", "", "evidence directory")
	flag.Parse()
	if *storeBin == "" || *minioBin == "" || *out == "" {
		return errors.New("--store-bin, --minio-bin and --out are required")
	}
	if err := os.MkdirAll(*out, 0o700); err != nil {
		return err
	}
	a, err := address()
	if err != nil {
		return err
	}
	console, err := address()
	if err != nil {
		return err
	}
	minio, err := start(*minioBin, []string{"server", filepath.Join(*out, "minio-data"), "--address", a, "--console-address", console}, filepath.Join(*out, "minio.log"), []string{"MINIO_ROOT_USER=storecachetest", "MINIO_ROOT_PASSWORD=storecache-test-secret", "MINIO_BROWSER=off"})
	if err != nil {
		return err
	}
	defer minio.stop(false)
	httpClient := &http.Client{Timeout: time.Second}
	ready := false
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		r, e := httpClient.Get("http://" + a + "/minio/health/live")
		if e == nil {
			r.Body.Close()
			if r.StatusCode == 200 {
				ready = true
				break
			}
		}
	}
	if !ready {
		return errors.New("MinIO not ready; inspect minio.log")
	}
	target, _ := url.Parse("http://" + a)
	proxy := httputil.NewSingleHostReverseProxy(target)
	var mu sync.Mutex
	var stallHeads atomic.Bool
	var holdRefresh atomic.Pointer[chan struct{}]
	var totals counts
	var events []map[string]any
	wire := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/chunk/") {
			mu.Lock()
			switch r.Method {
			case "HEAD":
				totals.Heads++
			case "PUT":
				totals.Puts++
			case "GET":
				totals.Gets++
			}
			events = append(events, map[string]any{"method": r.Method, "path": r.URL.Path, "at": time.Now().UTC()})
			mu.Unlock()
			if r.Method == http.MethodHead && stallHeads.Load() {
				<-r.Context().Done()
				return
			}
		}
		if r.Method == http.MethodHead {
			if held := holdRefresh.Load(); held != nil {
				select {
				case <-*held:
				case <-r.Context().Done():
					return
				}
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	defer wire.Close()
	defer func() {
		if held := holdRefresh.Swap(nil); held != nil {
			close(*held)
		}
	}()
	count := func() counts { mu.Lock(); defer mu.Unlock(); return totals }
	api := awss3.New(awss3.Options{Region: "us-east-1", BaseEndpoint: aws.String(wire.URL), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("storecachetest", "storecache-test-secret", "")})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const bucket = "store-cache-probe"
	if _, err = api.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	runtimeDir, err := os.MkdirTemp("/run/store-ctl", "manual-metadata-")
	if os.IsNotExist(err) {
		if err = os.MkdirAll("/run/store-ctl", 0o700); err == nil {
			runtimeDir, err = os.MkdirTemp("/run/store-ctl", "manual-metadata-")
		}
	}
	if err != nil {
		return err
	}
	namespace := stores3.CacheNamespace(wire.URL, "us-east-1", bucket, "probe", true)
	defaultNamespace := filepath.Join("/run/store-ctl/chunk-metadata-cache", namespace)
	defer os.RemoveAll(defaultNamespace)
	defer func() { _ = os.RemoveAll(filepath.Join(runtimeDir, "ttl-cache")) }()
	snapshots := filepath.Join(runtimeDir, "stats.json")
	var current *child
	defer func() {
		if current != nil {
			_ = current.stop(true)
		}
	}()
	var addr string
	config := func(name, cache, stats string) (string, error) {
		var err error
		addr, err = address()
		if err != nil {
			return "", err
		}
		text := fmt.Sprintf("listen: %s\nbackend: s3\nstats_interval: off\nstats_file: %s\nstats_file_interval: 20ms\ngenerations:\n  config: [G1]\ns3:\n  endpoint: %s\n  bucket: %s\n  prefix: probe\n  access_key: storecachetest\n  secret_key: storecache-test-secret\n%s", addr, stats, wire.URL, bucket, cache)
		name = filepath.Join(*out, name+".yaml")
		return name, os.WriteFile(name, []byte(text), 0o600)
	}
	launch := func(name, cache, stats string) error {
		cfg, err := config(name, cache, stats)
		if err != nil {
			return err
		}
		current, err = start(*storeBin, []string{"serve", "--config", cfg}, filepath.Join(*out, name+".log"), nil)
		if err != nil {
			return err
		}
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			c, e := storeclient.New(addr, 1, time.Second)
			if e != nil {
				continue
			}
			rctx, done := context.WithTimeout(ctx, 100*time.Millisecond)
			_, e = c.AdmitWrite(rctx)
			done()
			_ = c.Close()
			if e == nil {
				return nil
			}
		}
		return fmt.Errorf("store not ready: %s.log", name)
	}
	snapshot := func() (json.RawMessage, error) {
		b, e := os.ReadFile(snapshots)
		if e != nil {
			return nil, e
		}
		if !json.Valid(b) {
			return nil, errors.New("partial JSON snapshot")
		}
		return b, nil
	}
	waitSnapshot := func(minPuts, minIntercepted float64) error {
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
			b, e := snapshot()
			if e != nil {
				continue
			}
			var v struct {
				Chunk struct {
					PutRequests float64 `json:"put_requests_total"`
					Intercepted float64 `json:"cache_intercepted_total"`
				} `json:"chunk"`
			}
			if json.Unmarshal(b, &v) == nil && v.Chunk.PutRequests >= minPuts && v.Chunk.Intercepted >= minIntercepted {
				return nil
			}
		}
		return errors.New("stats_file failed to catch up")
	}
	payload := func(label string) []byte {
		return bytes.Repeat([]byte(label), ((64<<10)+len(label)-1)/len(label))[:64<<10]
	}
	put := func(data []byte) (bool, error) {
		c, e := storeclient.New(addr, 1, 5*time.Second)
		if e != nil {
			return false, e
		}
		defer c.Close()
		ad, e := c.AdmitWrite(ctx)
		if e != nil {
			return false, e
		}
		key := store.ContentKey(sha256.Sum256(data))
		return c.Put(ctx, ad, store.PartitionChunk, key, data)
	}
	verify := func(data []byte) error {
		c, e := storeclient.New(addr, 1, 5*time.Second)
		if e != nil {
			return e
		}
		defer c.Close()
		key := store.ContentKey(sha256.Sum256(data))
		found, body, e := c.Get(ctx, store.PartitionChunk, key)
		if e != nil {
			return e
		}
		return require(found && bytes.Equal(body, data), "chunk body mismatch")
	}
	results := make([]result, 0)
	record := func(name string, before counts, start time.Time) error {
		b, e := snapshot()
		if e != nil {
			return e
		}
		r := result{Name: name, Calls: count().sub(before), ElapsedMS: float64(time.Since(start).Microseconds()) / 1000, Snapshot: b}
		results = append(results, r)
		fmt.Printf("%s: HEAD=%d PUT=%d GET=%d %.3fms\n", name, r.Calls.Heads, r.Calls.Puts, r.Calls.Gets, r.ElapsedMS)
		return save(filepath.Join(*out, "results.json"), results)
	}

	// Same binary and workload, with the metadata cache explicitly disabled.
	baselineStats := filepath.Join(runtimeDir, "baseline-stats.json")
	if err = launch("disabled", "  chunk_metadata_cache:\n    enabled: false\n", baselineStats); err != nil {
		return err
	}
	snapshots = baselineStats
	before := count()
	begin := time.Now()
	dataA := payload("baseline")
	for i := 0; i < 100; i++ {
		fresh, e := put(dataA)
		if e != nil || fresh != (i == 0) {
			return fmt.Errorf("disabled Put %d: %v %v", i, fresh, e)
		}
	}
	if err = verify(dataA); err != nil {
		return err
	}
	if err = current.stop(false); err != nil {
		return err
	}
	current = nil
	if err = record("disabled-100", before, begin); err != nil {
		return err
	}
	if err = require(results[len(results)-1].Calls.Heads == 101 && results[len(results)-1].Calls.Puts == 1, "disabled request counts differ"); err != nil {
		return err
	}

	// Omit the entire cache block: default on, 150000 entries, 7 day max TTL.
	snapshots = filepath.Join(runtimeDir, "stats.json")
	if err = launch("enabled-defaults", "", snapshots); err != nil {
		return err
	}
	before = count()
	begin = time.Now()
	dataB := payload("withcache")
	for i := 0; i < 100; i++ {
		fresh, e := put(dataB)
		if e != nil || fresh != (i == 0) {
			return fmt.Errorf("enabled Put %d: %v %v", i, fresh, e)
		}
	}
	if err = verify(dataB); err != nil {
		return err
	}
	if err = waitSnapshot(100, 99); err != nil {
		return err
	}

	if err = record("enabled-defaults-100", before, begin); err != nil {
		return err
	}
	if err = require(results[len(results)-1].Calls.Heads == 2 && results[len(results)-1].Calls.Puts == 1, "enabled request counts differ"); err != nil {
		return err
	}
	if err = current.stop(true); err != nil {
		return err
	}
	current = nil
	records, err := readRecords(defaultNamespace)
	if err != nil || len(records) != 1 {
		return fmt.Errorf("expected one bbolt record: %d: %v", len(records), err)
	}
	var oldMeta []byte
	for _, raw := range records {
		oldMeta = raw
		if len(raw) != 196 || binary.LittleEndian.Uint64(raw[184:192])-binary.LittleEndian.Uint64(raw[176:184]) != uint64(168*time.Hour) {
			return errors.New("default max_ttl is not seven days")
		}
	}

	if err = launch("after-sigkill", "", snapshots); err != nil {
		return err
	}
	before = count()
	begin = time.Now()
	for i := 0; i < 100; i++ {
		fresh, e := put(dataB)
		if e != nil || fresh {
			return fmt.Errorf("reloaded Put %d: %v %v", i, fresh, e)
		}
	}
	if err = verify(dataB); err != nil {
		return err
	}
	if err = waitSnapshot(200, 199); err != nil {
		return err
	}

	if err = record("after-sigkill-100", before, begin); err != nil {
		return err
	}
	if err = require(results[len(results)-1].Calls.Heads == 0 && results[len(results)-1].Calls.Puts == 0, "reload failed to intercept"); err != nil {
		return err
	}

	before = count()
	begin = time.Now()
	dataC := payload("parallel")
	startGate := make(chan struct{})
	answers := make(chan error, 32)
	var freshMu sync.Mutex
	freshCount := 0
	for i := 0; i < 32; i++ {
		go func() {
			<-startGate
			fresh, e := put(dataC)
			if fresh {
				freshMu.Lock()
				freshCount++
				freshMu.Unlock()
			}
			answers <- e
		}()
	}
	close(startGate)
	for i := 0; i < 32; i++ {
		if e := <-answers; e != nil {
			return e
		}
	}
	if err = verify(dataC); err != nil {
		return err
	}
	if err = waitSnapshot(232, 230); err != nil {
		return err
	}
	if err = record("concurrent-32", before, begin); err != nil {
		return err
	}
	if freshCount != 1 || results[len(results)-1].Calls.Puts != 1 {
		return fmt.Errorf("parallel fresh=%d, calls=%+v", freshCount, results[len(results)-1].Calls)
	}

	// A memory hit needs no database access. Inject EFBIG only into this
	// isolated Store process; its small JSON snapshots remain writable.
	hotBefore := count()
	if fresh, e := put(dataC); e != nil || fresh {
		return fmt.Errorf("memory hit: %v %v", fresh, e)
	}
	if count().Heads != hotBefore.Heads {
		return errors.New("hot hit issued HEAD")
	}
	limit := exec.Command("prlimit", "--pid", fmt.Sprint(current.cmd.Process.Pid), "--fsize=4096:4096")
	if out, e := limit.CombinedOutput(); e != nil {
		return fmt.Errorf("inject bbolt write failure: %s: %w", out, e)
	}
	before = count()
	begin = time.Now()
	if fresh, e := put(bytes.Repeat([]byte("D"), 65536)); e != nil || !fresh {
		return fmt.Errorf("fill I/O failure: %v %v", fresh, e)
	}

	for i := 0; i < 2; i++ {
		if fresh, e := put(dataC); e != nil || fresh {
			return fmt.Errorf("bypass put: %v %v", fresh, e)
		}
	}
	if err = verify(dataC); err != nil {
		return err
	}
	if err = current.stop(false); err != nil {
		return err
	}
	current = nil
	restoredRecords, e := readRecords(defaultNamespace)
	if e != nil {
		return e
	}
	for id, previous := range records {
		if !bytes.Equal(restoredRecords[id], previous) {
			return errors.New("restart or hits rewrote/renewed metadata record")
		}
	}

	if err = record("cache-io-bypass", before, begin); err != nil {
		return err
	}
	if err = require(results[len(results)-1].Calls.Heads == 4 && results[len(results)-1].Calls.Puts == 1, "bypass request counts differ"); err != nil {
		return err
	}

	snapshots = filepath.Join(runtimeDir, "ttl-stats.json")
	ttlCfg := fmt.Sprintf("  chunk_metadata_cache:\n    dir: %s\n    max_ttl: 2s\n    max_entries: 2\n", filepath.Join(runtimeDir, "ttl-cache"))
	if err = launch("ttl-override", ttlCfg, snapshots); err != nil {
		return err
	}
	before = count()
	begin = time.Now()
	dataD := payload("expiry")
	if fresh, e := put(dataD); e != nil || !fresh {
		return fmt.Errorf("ttl initial: %v %v", fresh, e)
	}
	time.Sleep(2200 * time.Millisecond)
	if fresh, e := put(dataD); e != nil || fresh {
		return fmt.Errorf("ttl recheck: %v %v", fresh, e)
	}
	if err = verify(dataD); err != nil {
		return err
	}
	if err = current.stop(false); err != nil {
		return err
	}
	current = nil
	if err = record("ttl-expired", before, begin); err != nil {
		return err
	}
	if err = require(results[len(results)-1].Calls.Heads == 3 && results[len(results)-1].Calls.Puts == 1, "TTL did not HEAD again"); err != nil {
		return err
	}

	// Real access-triggered verification: keep HEAD blocked while foreground
	// cache hits return, queue two other keys, and check the renewed deadline.
	refreshDir := filepath.Join(runtimeDir, "refresh-cache")
	defer os.RemoveAll(refreshDir)
	refreshCfg := fmt.Sprintf("  chunk_metadata_cache:\n    dir: %s\n    max_ttl: 16s\n    max_entries: 4\n", refreshDir)
	snapshots = filepath.Join(runtimeDir, "refresh-stats.json")
	if err = launch("access-refresh", refreshCfg, snapshots); err != nil {
		return err
	}
	before = count()
	begin = time.Now()
	refreshData := [][]byte{payload("refresh-one"), payload("refresh-two"), payload("refresh-three")}
	for _, data := range refreshData {
		if fresh, e := put(data); e != nil || !fresh {
			return fmt.Errorf("refresh initial: %v %v", fresh, e)
		}
	}
	// Read the database only while its owning process is stopped. bbolt
	// deliberately excludes a second process opening a live writer's file.
	if err = current.stop(false); err != nil {
		return err
	}
	current = nil
	originals, e := readRecords(filepath.Join(refreshDir, namespace))
	if e != nil || len(originals) != 3 {
		return fmt.Errorf("refresh records: %d: %v", len(originals), e)
	}
	latestConfirm, latestExpiry := int64(0), int64(0)
	for _, raw := range originals {
		if len(raw) != 196 {
			return errors.New("invalid refresh record size")
		}
		latestConfirm = max(latestConfirm, int64(binary.LittleEndian.Uint64(raw[176:184])))
		latestExpiry = max(latestExpiry, int64(binary.LittleEndian.Uint64(raw[184:192])))
	}
	if err = launch("access-refresh-running", refreshCfg, snapshots); err != nil {
		return err
	}

	time.Sleep(max(0, time.Until(time.Unix(0, latestConfirm).Add(14*time.Second+10*time.Millisecond))))
	if got := count().sub(before); got.Heads != 6 || got.Puts != 3 {
		return fmt.Errorf("idle cache issued verification: %+v", got)
	}
	held := make(chan struct{})
	holdRefresh.Store(&held)
	for _, data := range refreshData {
		// These requests must complete even though their verification cannot finish.
		if fresh, e := put(data); e != nil || fresh {
			return fmt.Errorf("soft hit waited for HEAD: %v %v", fresh, e)
		}
	}
	for i := 0; i < 10; i++ {
		if fresh, e := put(refreshData[0]); e != nil || fresh {
			return fmt.Errorf("duplicate soft hit: %v %v", fresh, e)
		}
	}
	queueObserved := false
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		b, e := snapshot()
		if e != nil {
			continue
		}
		var v struct {
			Cache struct {
				Queue int `json:"refresh_queue_entries"`
			} `json:"chunk_metadata_cache"`
		}
		if json.Unmarshal(b, &v) == nil && v.Cache.Queue == 2 && count().sub(before).Heads == 7 {
			queueObserved = true
			break
		}
	}
	if !queueObserved {
		return errors.New("single-worker queue/dedup was not observed")
	}
	close(*holdRefresh.Swap(nil))
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline) && count().sub(before).Heads < 9; time.Sleep(10 * time.Millisecond) {
	}

	time.Sleep(max(0, time.Until(time.Unix(0, latestExpiry).Add(20*time.Millisecond))))
	for _, data := range refreshData {
		if fresh, e := put(data); e != nil || fresh {
			return fmt.Errorf("renewed hit: %v %v", fresh, e)
		}
		if e := verify(data); e != nil {
			return e
		}
	}
	if err = waitSnapshot(19, 16); err != nil {
		return err
	}
	if err = current.stop(true); err != nil {
		return err
	}
	current = nil
	renewed, e := readRecords(filepath.Join(refreshDir, namespace))
	if e != nil || len(renewed) != 3 {
		return fmt.Errorf("read renewed records: %v", e)
	}
	for id, previous := range originals {
		raw := renewed[id]
		if len(raw) != 196 || binary.LittleEndian.Uint64(raw[184:192]) <= binary.LittleEndian.Uint64(previous[184:192]) {
			return errors.New("verification did not persist renewed deadlines")
		}
	}

	if err = record("access-refresh-19", before, begin); err != nil {
		return err
	}
	if got := results[len(results)-1].Calls; got.Heads != 9 || got.Puts != 3 || got.Gets != 3 {
		return fmt.Errorf("refresh requests differ: %+v", got)
	}
	var refreshStats struct {
		Chunk struct {
			Heads uint64 `json:"s3_head_calls_total"`
		} `json:"chunk"`
	}
	if err = json.Unmarshal(results[len(results)-1].Snapshot, &refreshStats); err != nil || refreshStats.Chunk.Heads != 9 {
		return errors.New("background HEAD missing from total metrics")
	}
	if err = launch("after-refresh-sigkill", refreshCfg, snapshots); err != nil {
		return err
	}
	before = count()
	begin = time.Now()
	for _, data := range refreshData {
		if fresh, e := put(data); e != nil || fresh {
			return fmt.Errorf("restart after refresh: %v %v", fresh, e)
		}
		if e := verify(data); e != nil {
			return e
		}
	}

	if err = current.stop(false); err != nil {
		return err
	}
	current = nil
	reloaded, e := readRecords(filepath.Join(refreshDir, namespace))
	if e != nil || len(reloaded) != len(renewed) {
		return fmt.Errorf("read restored database: %v", e)
	}
	for id, previous := range renewed {
		if !bytes.Equal(reloaded[id], previous) {
			return errors.New("restart changed refreshed expiry")
		}
	}

	if err = record("after-refresh-sigkill", before, begin); err != nil {
		return err
	}
	if got := results[len(results)-1].Calls; got.Heads != 0 || got.Puts != 0 || got.Gets != 3 {
		return fmt.Errorf("refreshed cache did not reload: %+v", got)
	}

	// Actual SDK/HTTP degradation: keep HEAD unanswered until the client
	// cancels. The 32 requests must not form a 32 x 200ms gate timeout chain.
	snapshots = filepath.Join(runtimeDir, "stalled-stats.json")
	if err = launch("stalled-head", "  op_timeout: 200ms\n", snapshots); err != nil {
		return err
	}
	before = count()
	begin = time.Now()
	stallHeads.Store(true)
	faults := make(chan error, 32)
	faultStart := make(chan struct{})
	for i := 0; i < 32; i++ {
		go func() {
			<-faultStart
			fresh, e := put(payload("stalled-head"))
			if e == nil {
				faults <- nil
				return
			}
			_ = fresh
			faults <- e
		}()
	}
	close(faultStart)
	for i := 0; i < 32; i++ {
		select {
		case e := <-faults:
			if e == nil {
				return errors.New("missing timeout")
			}
		case <-time.After(2 * time.Second):
			return errors.New("same-key stalled HTTP HEAD formed a timeout chain")
		}
	}
	elapsed := time.Since(begin)
	stallHeads.Store(false)
	if elapsed > 2*time.Second {
		return fmt.Errorf("32 keyed stages exceeded shared budgets: %v", elapsed)
	}
	if err = current.stop(false); err != nil {
		return err
	}
	current = nil
	if err = record("stalled-head-32", before, begin); err != nil {
		return err
	}
	if results[len(results)-1].Calls.Puts != 0 || results[len(results)-1].Calls.Heads == 0 {
		return errors.New("timed-out HEAD caused a PUT")
	}

	// Publish all network events and the record's original fixed deadline.
	mu.Lock()
	err = save(filepath.Join(*out, "s3-http-events.json"), events)
	mu.Unlock()
	if err != nil {
		return err
	}
	details := map[string]any{"runtime_dir": runtimeDir, "default_cache_namespace": defaultNamespace, "confirmed_unix_ns": binary.LittleEndian.Uint64(oldMeta[176:184]), "expires_unix_ns": binary.LittleEndian.Uint64(oldMeta[184:192]), "record_sha256": hex.EncodeToString(sha256Bytes(oldMeta)), "all_payloads_verified": true, "minio_source": "github.com/minio/minio@v0.0.0-20250907161309-07c3a429bfed"}
	if err = save(filepath.Join(*out, "details.json"), details); err != nil {
		return err
	}
	fmt.Printf("PASS: evidence=%s metrics=%s\n", *out, filepath.Join(runtimeDir, "stats.json"))
	return nil
}
func sha256Bytes(b []byte) []byte { s := sha256.Sum256(b); return s[:] }

// The caller must first stop the process owning this namespace.
func readRecords(namespaceDir string) (map[string][]byte, error) {
	db, err := bolt.Open(filepath.Join(namespaceDir, "metadata.db"), 0600, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	defer db.Close()
	records := map[string][]byte{}
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("entries"))
		if b == nil {
			return errors.New("entries bucket missing")
		}
		return b.ForEach(func(key, value []byte) error {
			records[fmt.Sprintf("%x", key)] = bytes.Clone(value)
			return nil
		})
	})
	return records, err
}
