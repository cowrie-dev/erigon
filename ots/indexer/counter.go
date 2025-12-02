// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package indexer

import (
	"encoding/binary"

	"github.com/erigontech/erigon/common/length"
)

func OptimizedCounterSerializer(count uint64) []byte {
	v := make([]byte, 1) // 1 byte (counter - 1) [0, 255]
	v[0] = byte(count - 1)
	return v
}

func RegularCounterSerializer(count uint64, chunk []byte) []byte {
	// key == address
	// value (dup) == accumulated counter uint64 + chunk uint64
	v := make([]byte, length.Counter+length.Chunk)
	binary.BigEndian.PutUint64(v, count)
	copy(v[length.Counter:], chunk)
	return v
}

func LastCounterSerializer(count uint64) []byte {
	res := make([]byte, length.Counter+length.Chunk)
	binary.BigEndian.PutUint64(res, count)
	binary.BigEndian.PutUint64(res[length.Counter:], ^uint64(0))

	return res
}
