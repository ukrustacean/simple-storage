package datastore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type RecordKind byte

// magic numbers
const (
	Text   RecordKind = iota
	Number RecordKind = iota
)

type entry struct {
	key, value string
	kind       RecordKind
}

// 0           4    8     kl+8  kl+12   kl + vl + 12  <-- offset
// (full size) (kl) (key) (vl)  (value) (type)
// 4           4    ....  4     .....   1  <-- length
//                                      256 types possible

func (e *entry) Encode() []byte {
	kl, vl := len(e.key), len(e.value)
	size := kl + vl + 13 // 12 + 1 (type)
	res := make([]byte, size)

	binary.LittleEndian.PutUint32(res[0:], uint32(size))
	binary.LittleEndian.PutUint32(res[4:], uint32(kl))
	copy(res[8:], e.key)
	binary.LittleEndian.PutUint32(res[kl+8:], uint32(vl))
	copy(res[kl+12:], e.value)
	res[kl+vl+12] = byte(e.kind)

	return res
}

func (e *entry) Decode(input []byte) {
	kl := int(binary.LittleEndian.Uint32(input[4:]))
	vl := int(binary.LittleEndian.Uint32(input[kl+8:]))
	e.key = string(input[8 : 8+kl])
	e.value = string(input[kl+12 : kl+12+vl])
	e.kind = RecordKind(input[kl+vl+12])
}

func (e *entry) DecodeFromReader(in *bufio.Reader) (int, error) {
	sizeBuf, err := in.Peek(4)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, err
		}
		return 0, fmt.Errorf("cannot read size: %w", err)
	}

	size := int(binary.LittleEndian.Uint32(sizeBuf))
	buf := make([]byte, size)

	n, err := in.Read(buf)
	if err != nil {
		return n, fmt.Errorf("cannot read record: %w", err)
	}

	e.Decode(buf)
	return n, nil
}
