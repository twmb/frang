// SPDX-License-Identifier: Apache-2.0
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/util/pool/pool_test.go
// Provenance-includes-copyright: The Prometheus Authors

package pool

import (
	"testing"
)

func makeFunc(size int) []int {
	return make([]int, 0, size)
}

func TestBucketedPool_HappyPath(t *testing.T) {
	testPool := NewBucketedPool(1, 8, 2, makeFunc)
	cases := []struct {
		size        int
		expectedCap int
	}{
		{
			size:        -1,
			expectedCap: 1,
		},
		{
			size:        3,
			expectedCap: 4,
		},
		{
			size:        10,
			expectedCap: 10,
		},
	}
	for _, c := range cases {
		ret := testPool.Get(c.size)
		if cap(ret) < c.expectedCap {
			t.Fatalf("expected cap >= %d, got %d", c.expectedCap, cap(ret))
		}
		testPool.Put(ret)
	}
}

func TestBucketedPool_SliceNotAlignedToBuckets(t *testing.T) {
	pool := NewBucketedPool(1, 1000, 10, makeFunc)
	pool.Put(make([]int, 0, 2))
	s := pool.Get(3)
	if cap(s) < 3 {
		t.Fatalf("expected cap >= 3, got %d", cap(s))
	}
}

func TestBucketedPool_PutEmptySlice(t *testing.T) {
	pool := NewBucketedPool(1, 1000, 10, makeFunc)
	pool.Put([]int{})
	s := pool.Get(1)
	if cap(s) < 1 {
		t.Fatalf("expected cap >= 1, got %d", cap(s))
	}
}

func TestBucketedPool_PutSliceSmallerThanMinimum(t *testing.T) {
	pool := NewBucketedPool(3, 1000, 10, makeFunc)
	pool.Put([]int{1, 2})
	s := pool.Get(3)
	if cap(s) < 3 {
		t.Fatalf("expected cap >= 3, got %d", cap(s))
	}
}
