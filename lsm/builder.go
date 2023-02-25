package lsm

import (
	"errors"
	"fmt"
	"math"
	"unsafe"

	"zkv-go/utils"
)

type tableBuilder struct {
	sstSize       int64
	curBlock      *block
	opt           *Options
	blockList     []*block
	keyCount      uint32
	keyHashes     []uint32
	maxVersion    uint64
	baseKey       []byte
	staleDataSize int
	estimateSz    int64
}

type block struct {
	offset            int
	checksum          []byte
	chkLen            int
	entrierIndexStart int
	entryOffsets      []uint32
	baseKey           []byte
	end               int
	estimateSz        int64
	data              []byte
}
type header struct {
	overlap uint16 // Overlap with base key.
	diff    uint16 // Length of the diff.
}

const headerSize = uint16(unsafe.Sizeof(header{}))

// Decode decodes the header.
func (h *header) decode(buf []byte) {
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}
func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}

func (tb *tableBuilder) add(e *utils.Entry, isStale bool) {
	key := e.Key
	val := utils.ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}
	// 检查是否需要分配一个新的 block
	if tb.tryFinishBlock(e) {
		if isStale {
			// This key will be added to tableIndex and it is stale.
			tb.staleDataSize += len(key) + 4 /* len */ + 4 /* offset */
		}
		tb.finishBlock()
		// Create a new block and start writing.
		tb.curBlock = &block{
			data: make([]byte, tb.opt.BlockSize), // TODO 加密block后块的大小会增加，需要预留一些填充位置
		}
	}
	tb.keyHashes = append(tb.keyHashes, utils.Hash(utils.ParseKey(key)))

	if version := utils.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.keyDiff(key)
	}
	utils.CondPanic(!(len(key)-len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(key)-len(diffKey) <= math.MaxUint16"))
	utils.CondPanic(!(len(diffKey) <= math.MaxUint16), fmt.Errorf("tableBuilder.add: len(diffKey) <= math.MaxUint16"))

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}

	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	tb.append(h.encode())
	tb.append(diffKey)

	dst := tb.allocate(int(val.EncodedSize()))
	val.EncodeValue(dst)
}
func newTableBuilerWithSSTSize(opt *Options, size int64) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: size,
	}
}
func newTableBuiler(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: opt.SSTableMaxSz,
	}
}
func (tb *tableBuilder) empty() bool { return len(tb.keyHashes) == 0 }
func (tb *tableBuilder) finish() []byte {
	bd := tb.done()
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written == len(buf), nil)
	return buf
}
func (tb *tableBuilder) tryFinishBlock(e *utils.Entry) bool {
	if tb.curBlock == nil {
		return true
	}

	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}
	utils.CondPanic(!((uint32(len(tb.curBlock.entryOffsets))+1)*4+4+8+4 < math.MaxUint32), errors.New("Integer overflow"))
	entriesOffsetsSize := int64((len(tb.curBlock.entryOffsets)+1)*4 +
		4 + // size of list
		8 + // Sum64 in checksum proto
		4) // checksum length
	tb.curBlock.estimateSz = int64(tb.curBlock.end) + int64(6 /*header size for entry*/) +
		int64(len(e.Key)) + int64(e.EncodedSize()) + entriesOffsetsSize

	// Integer overflow check for table size.
	utils.CondPanic(!(uint64(tb.curBlock.end)+uint64(tb.curBlock.estimateSz) < math.MaxUint32), errors.New("Integer overflow"))

	return tb.curBlock.estimateSz > int64(tb.opt.BlockSize)
}

// AddStaleKey 记录陈旧key所占用的空间大小，用于日志压缩时的决策
func (tb *tableBuilder) AddStaleKey(e *utils.Entry) {
	// Rough estimate based on how much space it will occupy in the SST.
	tb.staleDataSize += len(e.Key) + len(e.Value) + 4 /* entry offset */ + 4 /* header size */
	tb.add(e, true)
}

// AddKey _
func (tb *tableBuilder) AddKey(e *utils.Entry) {
	tb.add(e, false)
}

// Close closes the TableBuilder.
func (tb *tableBuilder) Close() {
	// 结合内存分配器
}
func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}
	// Append the entryOffsets and its length.
	tb.append(utils.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(utils.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])

	// Append the block checksum and its length.
	tb.append(checksum)
	tb.append(utils.U32ToBytes(uint32(len(checksum))))
	tb.estimateSz += tb.curBlock.estimateSz
	tb.blockList = append(tb.blockList, tb.curBlock)
	// TODO: 预估整理builder写入磁盘后，sst文件的大小
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil // 表示当前block 已经被序列化到内存
	return
}

// append appends to curBlock.data
func (tb *tableBuilder) append(data []byte) {
	dst := tb.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}
func (tb *tableBuilder) allocate(need int) []byte {
	bb := tb.curBlock
	if len(bb.data[bb.end:]) < need {
		// We need to reallocate.
		sz := 2 * len(bb.data)
		if bb.end+need > sz {
			sz = bb.end + need
		}
		tmp := make([]byte, sz) // todo 这里可以使用内存分配器来提升性能
		copy(tmp, bb.data)
		bb.data = tmp
	}
	bb.end += need
	return bb.data[bb.end-need : bb.end]
}

func (tb *tableBuilder) calculateChecksum(data []byte) []byte {
	checkSum := utils.CalculateChecksum(data)
	return utils.U64ToBytes(checkSum)
}

func (tb *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(tb.curBlock.baseKey); i++ {
		if newKey[i] != tb.curBlock.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}
func (tb *tableBuilder) flush(lm *levelManager, tableName string) (t *table, err error)
