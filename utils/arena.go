package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

const MaxNodeSize = int(unsafe.Sizeof(node{}))
const (
	nodeAlign  = int(unsafe.Sizeof(uint64(0))) - 1
	offsetSize = int(unsafe.Sizeof(uint32(0)))
)

// Arena 结构体
type Arena struct {
	n          uint32
	shouldGrow bool
	buf        []byte
}

// 创建一个Arena
func newArena(n int64) *Arena {

	out := &Arena{
		n:   1,
		buf: make([]byte, n),
	}
	return out
}

// Arena 分配接口

func (s *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&s.n, sz)
	if !s.shouldGrow {
		AssertTrue(int(offset) <= len(s.buf))

	}

	if int(offset) > len(s.buf)-MaxNodeSize {
		growBy := uint32(len(s.buf))
		if growBy > 1<<30 {
			growBy = 1 << 30
		}

		if growBy < sz {
			growBy = sz
		}

		newbuf := make([]byte, len(s.buf)+int(growBy))
		AssertTrue(len(s.buf) == copy(newbuf, s.buf))
		s.buf = newbuf
	}
	return offset - sz
}

// 返回当前已分配大小
func (s *Arena) size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

// 将key放入内存池 返回偏移
func (s *Arena) putKey(key []byte) uint32 {
	keySize := len(key)
	offset := s.allocate(uint32(keySize))
	buf := s.buf[offset : offset+uint32(keySize)]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

// 将val放入内存池，返回偏移
func (s *Arena) putVal(v ValueStruct) uint32 {
	l := uint32(v.EncodedSize())
	offset := s.allocate(l)
	buf := s.buf[offset:]
	v.EncodeValue(buf)
	return offset

}
func (s *Arena) putNode(height int) uint32 {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - height) * offsetSize

	// Pad the allocation with enough bytes to ensure pointer alignment.
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := s.allocate(l)

	// Return the aligned offset.
	m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return m
}
func (s *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.buf[offset]))
}
func (s *Arena) getKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}
func (s *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
	ret.DecodeValue(s.buf[offset : offset+size])
	return
}
func (s *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0 //返回空指针
	}

	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&s.buf[0])))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
