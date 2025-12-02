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
	"bytes"
	"testing"

	"github.com/erigontech/erigon/common/hexutil"
)

func checkCounter(t *testing.T, result []byte, expected string) {
	if !bytes.Equal(result, hexutility.MustDecodeHex(expected)) {
		t.Errorf("got %s expected %s", hexutility.Encode(result), expected)
	}
}

func TestOptimizedCounterSerializerMin(t *testing.T) {
	r := OptimizedCounterSerializer(1)
	expected := "0x00"
	checkCounter(t, r, expected)
}

func TestOptimizedCounterSerializerMax(t *testing.T) {
	r := OptimizedCounterSerializer(256)
	expected := "0xff"
	checkCounter(t, r, expected)
}

func TestRegularCounterSerializer(t *testing.T) {
	r := RegularCounterSerializer(257, hexutility.MustDecodeHex("0x1234567812345678"))
	expected := "0x00000000000001011234567812345678"
	checkCounter(t, r, expected)
}

func TestLastCounterSerializerMin(t *testing.T) {
	r := LastCounterSerializer(0)
	expected := "0x0000000000000000ffffffffffffffff"
	checkCounter(t, r, expected)
}

func TestLastCounterSerializerMax(t *testing.T) {
	r := LastCounterSerializer(^uint64(0))
	expected := "0xffffffffffffffffffffffffffffffff"
	checkCounter(t, r, expected)
}
