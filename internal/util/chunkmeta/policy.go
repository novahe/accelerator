// Package chunkmeta shares cache identity and refresh policy between config,
// serving, metrics and offline administration without importing a backend.
package chunkmeta

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path"
	"strings"
	"time"
)

func Namespace(endpoint, region, bucket, prefix string, pathStyle bool) string {
	data, _ := json.Marshal([]any{"chunk-metadata-v1", endpoint, region, bucket, path.Clean(strings.Trim(prefix, "/")), pathStyle})
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func RefreshInterval(qps float64) (time.Duration, error) {
	nanos := float64(time.Second) / qps
	if qps <= 0 || math.IsNaN(qps) || math.IsInf(qps, 0) || nanos >= float64(math.MaxInt64) {
		return 0, errors.New("s3: invalid refresh_ahead.max_qps")
	}
	return time.Duration(max(1, math.Ceil(nanos))), nil
}
