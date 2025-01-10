package kadm

import (
	"math"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestDecodeACLOperations(t *testing.T) {
	tests := []struct {
		name     string
		bitfield int32
		expected []ACLOperation
	}{
		{
			name:     "Example 264",
			bitfield: 264,
			expected: []ACLOperation{
				kmsg.ACLOperationRead,
				kmsg.ACLOperationDescribe,
			},
		},
		{
			name:     "Example 3400",
			bitfield: 3400,
			expected: []ACLOperation{
				kmsg.ACLOperationRead,
				kmsg.ACLOperationDescribe,
				kmsg.ACLOperationAlterConfigs,
				kmsg.ACLOperationDescribeConfigs,
				kmsg.ACLOperationDelete,
			},
		},
		{
			name:     "Example 3968",
			bitfield: 3968,
			expected: []ACLOperation{
				kmsg.ACLOperationAlter,
				kmsg.ACLOperationAlterConfigs,
				kmsg.ACLOperationClusterAction,
				kmsg.ACLOperationDescribe,
				kmsg.ACLOperationDescribeConfigs,
			},
		},
		{
			name:     "Example 4000",
			bitfield: 4000,
			expected: []ACLOperation{
				kmsg.ACLOperationAlter,
				kmsg.ACLOperationAlterConfigs,
				kmsg.ACLOperationClusterAction,
				kmsg.ACLOperationCreate,
				kmsg.ACLOperationDescribe,
				kmsg.ACLOperationDescribeConfigs,
			},
		},
		{
			name:     "All Operations",
			bitfield: math.MaxInt32, // All bits set
			expected: []ACLOperation{
				kmsg.ACLOperationRead,
				kmsg.ACLOperationWrite,
				kmsg.ACLOperationCreate,
				kmsg.ACLOperationDelete,
				kmsg.ACLOperationAlter,
				kmsg.ACLOperationDescribe,
				kmsg.ACLOperationClusterAction,
				kmsg.ACLOperationDescribeConfigs,
				kmsg.ACLOperationAlterConfigs,
				kmsg.ACLOperationIdempotentWrite,
				kmsg.ACLOperationCreateTokens,
				kmsg.ACLOperationDescribeTokens,
			},
		},
		{
			name:     "Invalid Operations Excluded",
			bitfield: 1<<15 | 1<<16, // Bits beyond known operations
			expected: []ACLOperation{},
		},
		{
			name:     "Empty Bitfield",
			bitfield: math.MinInt32,
			expected: []ACLOperation{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := DecodeACLOperations(tt.bitfield)

			// Compare slices ignoring order
			expectedMap := make(map[kmsg.ACLOperation]bool)
			for _, op := range tt.expected {
				expectedMap[op] = true
			}

			resultMap := make(map[kmsg.ACLOperation]bool)
			for _, op := range result {
				resultMap[op] = true
			}

			if !reflect.DeepEqual(expectedMap, resultMap) {
				t.Errorf("DecodeACLOperations(%d) = %v, expected %v", tt.bitfield, result, tt.expected)
			}
		})
	}
}
