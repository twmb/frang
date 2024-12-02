package kgo

import (
	"bytes"
	"sync"
)

// shared general purpose byte buff
var byteBuffers = sync.Pool{New: func() any { return bytes.NewBuffer(make([]byte, 8<<10)) }}
