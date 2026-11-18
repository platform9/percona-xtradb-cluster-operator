package pxc

import (
	"reflect"
	"testing"
)

const (
	uuidA = "af232558-d6ef-11f1-bd98-6fed12941d39"
	uuidB = "0c1f6a3e-1b2c-11f0-9a5e-2f4c1d7e8b90"
)

func TestParseRecoveryPosition(t *testing.T) {
	tests := []struct {
		name    string
		line    string
		want    recoveryPosition
		wantErr bool
	}{
		{
			name: "entrypoint without uuid",
			line: logPrefix + ":cluster1-pxc-0.cluster1-pxc.ns:1234:#####",
			want: recoveryPosition{seq: 1234},
		},
		{
			name: "entrypoint with uuid",
			line: logPrefix + ":cluster1-pxc-0.cluster1-pxc.ns:1234:" + uuidA + ":#####",
			want: recoveryPosition{seq: 1234, uuid: uuidA},
		},
		{
			name: "unknown seqno with empty uuid",
			line: logPrefix + ":cluster1-pxc-2.cluster1-pxc.ns:-1::#####",
			want: recoveryPosition{seq: -1},
		},
		{
			name:    "bad seqno",
			line:    logPrefix + ":cluster1-pxc-0.cluster1-pxc.ns:abc:#####",
			wantErr: true,
		},
		{
			name:    "too many fields",
			line:    logPrefix + ":a:1:b:c:#####",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRecoveryPosition(tt.line)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestConflictingHistories(t *testing.T) {
	tests := []struct {
		name      string
		positions map[string]recoveryPosition
		want      []string
	}{
		{
			name: "same history",
			positions: map[string]recoveryPosition{
				"pxc-0": {seq: 10, uuid: uuidA}, "pxc-1": {seq: 12, uuid: uuidA}, "pxc-2": {seq: 11, uuid: uuidA},
			},
		},
		{
			name: "old entrypoints report no uuid",
			positions: map[string]recoveryPosition{
				"pxc-0": {seq: 10}, "pxc-1": {seq: 12}, "pxc-2": {seq: 11},
			},
		},
		{
			name: "mixed old and new entrypoints during an upgrade",
			positions: map[string]recoveryPosition{
				"pxc-0": {seq: 10, uuid: uuidA}, "pxc-1": {seq: 12}, "pxc-2": {seq: 11, uuid: uuidA},
			},
		},
		{
			name: "a node without history does not conflict",
			positions: map[string]recoveryPosition{
				"pxc-0": {seq: 10, uuid: uuidA}, "pxc-1": {seq: -1, uuid: zeroUUID}, "pxc-2": {seq: 11, uuid: uuidA},
			},
		},
		{
			name: "a node from another cluster history",
			positions: map[string]recoveryPosition{
				"pxc-0": {seq: 10, uuid: uuidA}, "pxc-1": {seq: 900000, uuid: uuidB}, "pxc-2": {seq: 11, uuid: uuidA},
			},
			want: []string{uuidB, uuidA},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := conflictingHistories(tt.positions); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}
