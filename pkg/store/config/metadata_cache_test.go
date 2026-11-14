package config

import (
	"strings"
	"testing"
)

func TestChunkMetadataCacheDefaultsAndOverrides(t *testing.T) {
	base := "backend: s3\ns3:\n  endpoint: https://objects.example\n  bucket: test\n"
	c, err := LoadConfig(writeConfig(t, base), false)
	if err != nil {
		t.Fatal(err)
	}
	m := c.S3.ChunkMetadataCache
	if !m.IsEnabled() || m.Dir != DefaultChunkMetadataCacheDir || m.MaxTTL != "168h" || *m.MaxEntries != 150000 || *c.StatsFile != DefaultStatsFile || c.StatsFileInterval != "5s" {
		t.Fatalf("defaults: %+v %+v", c, m)
	}
	if !m.RefreshAhead.IsEnabled() || *m.RefreshAhead.MaxQPS != 2 {
		t.Fatalf("refresh defaults: %+v", m.RefreshAhead)
	}
	c, err = LoadConfig(writeConfig(t, base+"  chunk_metadata_cache:\n    enabled: false\n    max_ttl: invalid\n    dir: relative\nstats_file: ''\n"), false)
	if err != nil || c.S3.ChunkMetadataCache.IsEnabled() || *c.StatsFile != "" {
		t.Fatalf("disable: %+v %v", c, err)
	}
	t.Setenv("META_CACHE_PATH", "/dev/shm/meta-override")
	c, err = LoadConfig(writeConfig(t, base+"  chunk_metadata_cache:\n    dir: ${META_CACHE_PATH}\n    max_ttl: 2m\n    max_entries: 17\nstats_file: ${META_CACHE_PATH}/metrics.json\nstats_file_interval: 1s\n"), false)
	if err != nil {
		t.Fatal(err)
	}
	m = c.S3.ChunkMetadataCache
	if !m.IsEnabled() || m.Dir != "/dev/shm/meta-override" || m.MaxTTL != "2m" || *m.MaxEntries != 17 || *c.StatsFile != "/dev/shm/meta-override/metrics.json" {
		t.Fatalf("override: %+v %+v", c, m)
	}
	clone := c.Clone()
	*clone.S3.ChunkMetadataCache.MaxEntries = 99
	*clone.StatsFile = "other"
	off := false
	clone.S3.ChunkMetadataCache.Enabled = &off
	clone.S3.ChunkMetadataCache.RefreshAhead.Enabled = &off
	*clone.S3.ChunkMetadataCache.RefreshAhead.MaxQPS = 7
	if *m.MaxEntries != 17 || *c.StatsFile == "other" || !m.IsEnabled() {
		t.Fatal("clone aliases cache configuration")
	}
	if !m.RefreshAhead.IsEnabled() || *m.RefreshAhead.MaxQPS != 2 {
		t.Fatal("clone aliases refresh configuration")
	}
}

func TestChunkRefreshOptions(t *testing.T) {
	base := "backend: s3\ns3:\n  endpoint: https://objects.example\n  bucket: test\n  chunk_metadata_cache:\n"
	for _, field := range []string{
		"    ttl: 192h\n", "    max_entry: 17\n",
		"    refresh_ahead:\n      max_inflight: 2\n",
		"    refresh_ahead:\n      max_qps: 0\n",
		"    refresh_ahead:\n      max_qps: -1\n",
		"    refresh_ahead:\n      max_qps: .nan\n",
		"    refresh_ahead:\n      max_qps: .inf\n",
		"    refresh_ahead:\n      max_qps: 1e-20\n",
	} {
		if _, err := LoadConfig(writeConfig(t, base+field), false); err == nil {
			t.Fatalf("accepted invalid option: %s", field)
		}
	}
	c, err := LoadConfig(writeConfig(t, base+"    max_ttl: 24h\n    refresh_ahead:\n      enabled: false\n"), false)
	if err != nil || c.S3.ChunkMetadataCache.RefreshAhead.IsEnabled() || c.S3.ChunkMetadataCache.MaxTTL != "24h" {
		t.Fatalf("disable refresh: %+v %v", c, err)
	}
	// App normalizes a clone of an already loaded config; normalization is idempotent.
	clone := c.Clone()
	if err := clone.normalizeMetadataCache(false); err != nil || clone.S3.ChunkMetadataCache.MaxTTL != "24h" {
		t.Fatalf("repeat normalization: %v", err)
	}
}

func TestChunkMetadataCacheRejectsInvalidActiveConfig(t *testing.T) {
	for _, field := range []string{"max_ttl: 0s", "max_ttl: nonsense", "max_entries: 0", "max_entries: -1", "dir: relative"} {
		t.Run(field, func(t *testing.T) {
			text := "backend: s3\ns3:\n  endpoint: https://objects.example\n  bucket: test\n  chunk_metadata_cache:\n    " + field + "\n"
			if _, err := LoadConfig(writeConfig(t, text), false); err == nil || !strings.Contains(err.Error(), "chunk_metadata_cache") {
				t.Fatalf("expected field error: %v", err)
			}
		})
	}
	for _, fields := range []string{"stats_file: relative", "stats_file_interval: 0s", "stats_file_interval: bad"} {
		if _, err := LoadConfig(writeConfig(t, "backend: s3\ns3:\n  endpoint: https://objects.example\n  bucket: test\n"+fields+"\n"), false); err == nil {
			t.Fatalf("accepted %s", fields)
		}
	}
}

func TestChunkMetadataCacheRequiresPositiveTimeout(t *testing.T) {
	base := "backend: s3\ns3:\n  endpoint: https://objects.example\n  bucket: test\n"
	c, err := LoadConfig(writeConfig(t, base), false)
	if err != nil {
		t.Fatal(err)
	}
	if c.S3.OpTimeout != "10s" {
		t.Fatalf("unbounded default with cache: %q", c.S3.OpTimeout)
	}
	for _, v := range []string{"0", "0s", "-1s", "invalid"} {
		if _, err = LoadConfig(writeConfig(t, base+"  op_timeout: "+v+"\n"), false); err == nil || !strings.Contains(err.Error(), "op_timeout") {
			t.Fatalf("accepted timeout %s: %v", v, err)
		}
	}
	c, err = LoadConfig(writeConfig(t, base+"  chunk_metadata_cache:\n    enabled: false\n"), false)
	if err != nil {
		t.Fatal(err)
	}
	timeout, err := c.S3OpTimeout()
	if err != nil || timeout != 0 {
		t.Fatalf("cache-off legacy changed: %v %v", timeout, err)
	}
}
