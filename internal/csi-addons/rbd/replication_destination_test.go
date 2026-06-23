/*
Copyright 2026 The Ceph-CSI Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rbd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/csi-addons/spec/lib/go/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestGetDestinationIDFromCSIID tests the CSI ID mapping logic for replication destinations.
func TestGetDestinationIDFromCSIID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name        string
		srcID       string
		clusterID   string
		poolName    string
		configJSON  string
		wantDestID  string
		wantErr     bool
		errContains string
	}{
		{
			name:      "valid mapping with pool remapping",
			srcID:     "0001-000f-primary-cluster-0000000000000001-00000000-1111-2222-3333-444444444444",
			clusterID: "primary-cluster",
			poolName:  "rbd",
			configJSON: `[{
				"clusterID": "primary-cluster",
				"monitors": ["10.0.0.1:6789"],
				"replicationDestination": {
					"remoteClusterID": "secondary-cluster",
					"rbd": {
						"remotePoolMapping": {
							"rbd": {"poolID": "5"}
						}
					}
				}
			}]`,
			wantDestID: "0001-0011-secondary-cluster-0000000000000005-00000000-1111-2222-3333-444444444444",
			wantErr:    false,
		},
		{
			name:      "valid mapping without pool remapping",
			srcID:     "0001-000f-primary-cluster-0000000000000001-00000000-1111-2222-3333-444444444444",
			clusterID: "primary-cluster",
			poolName:  "replicapool",
			configJSON: `[{
				"clusterID": "primary-cluster",
				"monitors": ["10.0.0.1:6789"],
				"replicationDestination": {
					"remoteClusterID": "secondary-cluster",
					"rbd": {
						"remotePoolMapping": {
							"rbd": {"poolID": "5"}
						}
					}
				}
			}]`,
			// Pool ID stays same (1) because replicapool has no mapping
			wantDestID: "0001-0011-secondary-cluster-0000000000000001-00000000-1111-2222-3333-444444444444",
			wantErr:    false,
		},
		{
			name:      "no destination configured returns same ID",
			srcID:     "0001-000f-primary-cluster-0000000000000001-00000000-1111-2222-3333-444444444444",
			clusterID: "primary-cluster",
			poolName:  "rbd",
			configJSON: `[{
				"clusterID": "primary-cluster",
				"monitors": ["10.0.0.1:6789"]
			}]`,
			wantDestID: "0001-000f-primary-cluster-0000000000000001-00000000-1111-2222-3333-444444444444",
			wantErr:    false,
		},
		{
			name:        "invalid source ID",
			srcID:       "invalid-csi-id",
			clusterID:   "primary-cluster",
			poolName:    "rbd",
			configJSON:  `[{"clusterID": "primary-cluster", "monitors": ["10.0.0.1:6789"]}]`,
			wantErr:     true,
			errContains: "failed to decompose source ID",
		},
		{
			name:      "empty remote cluster ID",
			srcID:     "0001-000f-primary-cluster-0000000000000001-00000000-1111-2222-3333-444444444444",
			clusterID: "primary-cluster",
			poolName:  "rbd",
			configJSON: `[{
				"clusterID": "primary-cluster",
				"monitors": ["10.0.0.1:6789"],
				"replicationDestination": {
					"remoteClusterID": "",
					"rbd": {"remotePoolMapping": {}}
				}
			}]`,
			wantErr:     true,
			errContains: "remote cluster ID is empty",
		},
		{
			name:      "invalid remote pool ID format",
			srcID:     "0001-000f-primary-cluster-0000000000000001-00000000-1111-2222-3333-444444444444",
			clusterID: "primary-cluster",
			poolName:  "rbd",
			configJSON: `[{
				"clusterID": "primary-cluster",
				"monitors": ["10.0.0.1:6789"],
				"replicationDestination": {
					"remoteClusterID": "secondary-cluster",
					"rbd": {
						"remotePoolMapping": {
							"rbd": {"poolID": "invalid"}
						}
					}
				}
			}]`,
			wantErr:     true,
			errContains: "invalid syntax",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create a unique config file for this test
			testConfigFile := filepath.Join(t.TempDir(), "config.json")
			err := os.WriteFile(testConfigFile, []byte(tt.configJSON), 0o600)
			require.NoError(t, err)

			destID, err := getDestinationIDFromCSIID(ctx, tt.srcID, tt.clusterID, tt.poolName, testConfigFile)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantDestID, destID)
			}
		})
	}
}

// TestGetReplicationDestinationInfo_RequestValidation tests request validation for GetReplicationDestinationInfo RPC.
func TestGetReplicationDestinationInfo_RequestValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	rs := &ReplicationServer{
		driverInstance: "test-driver",
	}

	tests := []struct {
		name        string
		req         *replication.GetReplicationDestinationInfoRequest
		wantCode    codes.Code
		errContains string
	}{
		{
			name:        "nil replication source",
			req:         &replication.GetReplicationDestinationInfoRequest{},
			wantCode:    codes.InvalidArgument,
			errContains: "replication source is required",
		},
		{
			name: "empty volume ID",
			req: &replication.GetReplicationDestinationInfoRequest{
				ReplicationSource: &replication.ReplicationSource{
					Type: &replication.ReplicationSource_Volume{
						Volume: &replication.ReplicationSource_VolumeSource{
							VolumeId: "",
						},
					},
				},
			},
			wantCode:    codes.InvalidArgument,
			errContains: "empty volume ID",
		},
		{
			name: "empty volume group ID",
			req: &replication.GetReplicationDestinationInfoRequest{
				ReplicationSource: &replication.ReplicationSource{
					Type: &replication.ReplicationSource_Volumegroup{
						Volumegroup: &replication.ReplicationSource_VolumeGroupSource{
							VolumeGroupId: "",
						},
					},
				},
			},
			wantCode:    codes.InvalidArgument,
			errContains: "empty volume group ID",
		},
		{
			name: "neither volume nor volume group specified",
			req: &replication.GetReplicationDestinationInfoRequest{
				ReplicationSource: &replication.ReplicationSource{
					Type: nil,
				},
			},
			wantCode:    codes.InvalidArgument,
			errContains: "either volume or volumegroup source must be specified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := rs.GetReplicationDestinationInfo(ctx, tt.req)

			require.Error(t, err)
			st, ok := status.FromError(err)
			require.True(t, ok, "error should be a gRPC status error")
			assert.Equal(t, tt.wantCode, st.Code())
			assert.Contains(t, st.Message(), tt.errContains)
		})
	}
}
