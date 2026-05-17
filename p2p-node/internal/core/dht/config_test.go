// p2p-node/internal/core/dht/config_test.go
package dht

import (
	"errors"
	"testing"
	"time"
)

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.K != DefaultBucketSize {
		t.Fatalf("unexpected default K: got %d, want %d", cfg.K, DefaultBucketSize)
	}

	if cfg.Alpha != DefaultAlpha {
		t.Fatalf("unexpected default Alpha: got %d, want %d", cfg.Alpha, DefaultAlpha)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config must be valid: %v", err)
	}
}

func TestConfigNormalizeFillsDefaults(t *testing.T) {
	cfg := Config{}.Normalize()

	if cfg.K != DefaultBucketSize {
		t.Fatalf("Normalize() did not set K: got %d", cfg.K)
	}

	if cfg.Alpha != DefaultAlpha {
		t.Fatalf("Normalize() did not set Alpha: got %d", cfg.Alpha)
	}

	if cfg.RequestTimeout != DefaultRequestTimeout {
		t.Fatalf("Normalize() did not set RequestTimeout: got %s", cfg.RequestTimeout)
	}

	if cfg.RecordTTL != DefaultRecordTTL {
		t.Fatalf("Normalize() did not set RecordTTL: got %s", cfg.RecordTTL)
	}
}

func TestConfigValidateRejectsAlphaGreaterThanK(t *testing.T) {
	cfg := DefaultConfig()
	cfg.K = 2
	cfg.Alpha = 3

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}

	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestConfigValidateRejectsRepublishNotLessThanTTL(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RecordTTL = time.Hour
	cfg.RepublishInterval = time.Hour

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error")
	}

	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestConfigValidateAcceptsCustomConfig(t *testing.T) {
	cfg := Config{
		K:                     8,
		Alpha:                 3,
		RequestTimeout:        time.Second,
		BucketRefreshInterval: time.Minute,
		RecordTTL:             24 * time.Hour,
		ReplicateInterval:     time.Hour,
		RepublishInterval:     12 * time.Hour,
		MaxFailures:           2,
		EnforceTrustedPeers:   true,
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("custom config must be valid: %v", err)
	}
}
