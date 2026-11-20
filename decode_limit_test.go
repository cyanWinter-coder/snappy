// Copyright 2024 The Snappy-Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package snappy

import (
	"bytes"
	"io"
	"io/ioutil"
	"strings"
	"testing"
)

// countingReader wraps an io.Reader and counts how many bytes it has handed
// out, so tests can assert that a budget-exceeded Reader stops consuming input.
type countingReader struct {
	r     io.Reader
	count int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.count += n
	return n, err
}

// limitedTestStream builds a snappy stream encoding src and returns its bytes
// together with the number of compressed bytes the framing consumes (everything
// up to and including the final data chunk).
func limitedTestStream(t *testing.T, src []byte) []byte {
	t.Helper()
	buf := new(bytes.Buffer)
	if _, err := NewWriter(buf).Write(src); err != nil {
		t.Fatalf("Write: %v", err)
	}
	return buf.Bytes()
}

// readAll reads r through successive Reads with a small buffer on purpose so
// the budget path is exercised across many Reads. io.EOF is reported as nil.
func readAllWithSmallBuffer(t *testing.T, r io.Reader) ([]byte, error) {
	t.Helper()
	out := make([]byte, 0)
	p := make([]byte, 7)
	for {
		n, err := r.Read(p)
		out = append(out, p[:n]...)
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
	}
}

func TestLimitedReaderEmptyInput(t *testing.T) {
	stream := limitedTestStream(t, nil)
	for _, limit := range []int64{0, 1, -1} {
		r := NewLimitedReader(bytes.NewReader(stream), limit)
		got, err := ioutil.ReadAll(r)
		if err != nil {
			t.Errorf("limit=%d: got %v, want nil", limit, err)
			continue
		}
		if len(got) != 0 {
			t.Errorf("limit=%d: got %d bytes, want 0", limit, len(got))
		}
	}
}

func TestLimitedReaderExactlyAtAndOverBudget(t *testing.T) {
	src := bytes.Repeat([]byte("abcdefgh"), 1000) // 8000 bytes.
	stream := limitedTestStream(t, src)

	for _, readByte := range []bool{false, true} {
		// Exactly at the budget: every byte is delivered, then io.EOF.
		r := NewLimitedReader(bytes.NewReader(stream), int64(len(src)))
		var got []byte
		var err error
		if readByte {
			got, err = readAllByByte(r)
		} else {
			got, err = ioutil.ReadAll(r)
		}
		if err != nil {
			t.Fatalf("readByte=%v at-limit: got %v, want nil EOF", readByte, err)
		}
		if !bytes.Equal(got, src) {
			t.Fatalf("readByte=%v at-limit: output mismatch (got %d bytes, want %d)", readByte, len(got), len(src))
		}

		// One byte over the budget: only budget bytes are delivered.
		limit := int64(len(src) - 1)
		r = NewLimitedReader(bytes.NewReader(stream), limit)
		if readByte {
			got, err = readAllByByte(r)
		} else {
			got, err = readAllWithSmallBuffer(t, r)
		}
		if err != ErrLimitExceeded {
			t.Fatalf("readByte=%v over-budget: got %v, want ErrLimitExceeded", readByte, err)
		}
		if int64(len(got)) != limit {
			t.Fatalf("readByte=%v over-budget: delivered %d bytes, want %d", readByte, len(got), limit)
		}
		if !bytes.Equal(got, src[:limit]) {
			t.Fatalf("readByte=%v over-budget: delivered prefix mismatch", readByte)
		}

		// The error is sticky: subsequent calls keep returning it.
		p := make([]byte, 16)
		for i := 0; i < 3; i++ {
			if n, err := r.Read(p); n != 0 || err != ErrLimitExceeded {
				t.Fatalf("readByte=%v repeat #%d: got (%d, %v), want (0, ErrLimitExceeded)", readByte, i, n, err)
			}
			if _, err := r.ReadByte(); err != ErrLimitExceeded {
				t.Fatalf("readByte=%v ReadByte repeat #%d: got %v, want ErrLimitExceeded", readByte, i, err)
			}
		}
	}
}

func readAllByByte(r io.ByteReader) ([]byte, error) {
	var out []byte
	for {
		c, err := r.ReadByte()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, c)
	}
}

func TestLimitedReaderZeroLimit(t *testing.T) {
	src := []byte("non-empty stream")
	stream := limitedTestStream(t, src)

	cr := &countingReader{r: bytes.NewReader(stream)}
	r := NewLimitedReader(cr, 0)
	n, err := r.Read(make([]byte, 32))
	if n != 0 || err != ErrLimitExceeded {
		t.Fatalf("first Read: got (%d, %v), want (0, ErrLimitExceeded)", n, err)
	}
	// After the error, no more input is consumed.
	consumed := cr.count
	if n, err := r.Read(make([]byte, 32)); n != 0 || err != ErrLimitExceeded {
		t.Fatalf("second Read: got (%d, %v), want (0, ErrLimitExceeded)", n, err)
	}
	if cr.count != consumed {
		t.Fatalf("input consumed after limit: %d extra bytes", cr.count-consumed)
	}
}

func TestLimitedReaderStopsConsumingInput(t *testing.T) {
	// Two data chunks, each 100 bytes, with the budget inside the first chunk's
	// worth of output.
	src := append(bytes.Repeat([]byte{'a'}, 100), bytes.Repeat([]byte{'b'}, 100)...)
	stream := limitedTestStream(t, src)

	cr := &countingReader{r: bytes.NewReader(stream)}
	r := NewLimitedReader(cr, 50)
	got, err := readAllWithSmallBuffer(t, r)
	if err != ErrLimitExceeded {
		t.Fatalf("got %v, want ErrLimitExceeded", err)
	}
	if len(got) != 50 || !bytes.Equal(got, src[:50]) {
		t.Fatalf("delivered %d bytes, want first 50", len(got))
	}
	consumed := cr.count
	// Repeated reads must not touch the underlying reader again.
	for i := 0; i < 3; i++ {
		if n, err := r.Read(make([]byte, 10)); n != 0 || err != ErrLimitExceeded {
			t.Fatalf("repeat #%d: got (%d, %v), want (0, ErrLimitExceeded)", i, n, err)
		}
	}
	if cr.count != consumed {
		t.Fatalf("consumed %d more bytes after the limit was hit", cr.count-consumed)
	}
}

func TestLimitedReaderBudgetDoesNotMaskCorruption(t *testing.T) {
	// Valid first chunk delivering 4 bytes, then a corrupt second chunk. The
	// budget (2) is exceeded while reading the valid chunk, but the Reader must
	// surface ErrLimitExceeded for the over-budget read without ever reporting
	// success over corrupt data; and when the budget is large enough that the
	// corrupt chunk is reached, corruption wins over the budget.
	good := magicChunk +
		"\x01\x08\x00\x00" + // Uncompressed chunk, 8 bytes long.
		"\x68\x10\xe6\xb6" + // Checksum of "abcd".
		"\x61\x62\x63\x64" // "abcd".

	// Truncated second chunk: header promises 8 bytes but none follow.
	corrupt := good + "\x01\x08\x00\x00"

	t.Run("truncated chunk reached", func(t *testing.T) {
		r := NewLimitedReader(strings.NewReader(corrupt), 100)
		got, err := ioutil.ReadAll(r)
		if err != ErrCorrupt {
			t.Fatalf("got %v, want ErrCorrupt", err)
		}
		if string(got) != "abcd" {
			t.Fatalf("got %q, want %q", got, "abcd")
		}
	})

	t.Run("budget hit before corruption", func(t *testing.T) {
		// Budget of 2: the first two bytes are delivered, then the limit is hit.
		// The corrupt chunk is never reached, so the limit error is reported.
		r := NewLimitedReader(strings.NewReader(corrupt), 2)
		got, err := readAllWithSmallBuffer(t, r)
		if err != ErrLimitExceeded {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
		if string(got) != "ab" {
			t.Fatalf("got %q, want %q", got, "ab")
		}
	})

	t.Run("illegal copy offset", func(t *testing.T) {
		// A compressed chunk whose snappy block payload references offset 0,
		// which is always invalid. Budget is generous so the chunk is decoded.
		// Block: decodedLen = 1 (varint 0x01), then tagCopy1 with offset 0.
		block := []byte{0x01, 0x02, 0x00}
		body := make([]byte, 0, checksumSize+len(block))
		body = append(body, 0, 0, 0, 0) // Checksum is irrelevant: decode fails first.
		body = append(body, block...)
		header := []byte{
			chunkTypeCompressedData,
			byte(len(body)),
			byte(len(body) >> 8),
			byte(len(body) >> 16),
		}
		stream := magicChunk + string(header) + string(body)
		r := NewLimitedReader(strings.NewReader(stream), 1_000_000)
		if _, err := ioutil.ReadAll(r); err != ErrCorrupt {
			t.Fatalf("got %v, want ErrCorrupt", err)
		}
	})

	t.Run("trailing extra bytes", func(t *testing.T) {
		// Trailing garbage after a valid stream: an unskippable reserved chunk
		// type (0x02) must produce ErrUnsupported, regardless of budget.
		stream := good + "\x02\x00\x00\x00"
		r := NewLimitedReader(strings.NewReader(stream), 1_000_000)
		if _, err := ioutil.ReadAll(r); err != ErrUnsupported {
			t.Fatalf("got %v, want ErrUnsupported", err)
		}
	})
}

func TestLimitedReaderBoundaryThenCorruption(t *testing.T) {
	// The budget is exactly the first chunk's 4 output bytes. When they have
	// all been delivered the Reader probes for the next chunk to tell EOF from
	// an over-budget stream; that probe must surface a corrupt chunk rather
	// than the limit error, since format errors take precedence.
	good := magicChunk +
		"\x01\x08\x00\x00" + // Uncompressed chunk, 8 bytes long.
		"\x68\x10\xe6\xb6" + // Checksum of "abcd".
		"\x61\x62\x63\x64" // "abcd".
	corruptNext := good + "\x01\x08\x00\x00" // Header promises 8 bytes, none follow.

	r := NewLimitedReader(strings.NewReader(corruptNext), 4)
	got, err := readAllWithSmallBuffer(t, r)
	if err != ErrCorrupt {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
	if string(got) != "abcd" {
		t.Fatalf("got %q, want %q", got, "abcd")
	}

	// With a valid stream ending exactly at the budget, the probe instead sees
	// io.EOF and that must win over ErrLimitExceeded.
	r2 := NewLimitedReader(strings.NewReader(good), 4)
	got, err = ioutil.ReadAll(r2)
	if err != nil {
		t.Fatalf("got %v, want nil EOF", err)
	}
	if string(got) != "abcd" {
		t.Fatalf("got %q, want %q", got, "abcd")
	}
}

func TestLimitedReaderResetRestartsBudget(t *testing.T) {
	src := bytes.Repeat([]byte{'z'}, 100)
	stream := limitedTestStream(t, src)

	r := NewLimitedReader(bytes.NewReader(stream), 10)
	if got, err := readAllWithSmallBuffer(t, r); err != ErrLimitExceeded || len(got) != 10 {
		t.Fatalf("first stream: got (%d bytes, %v), want (10, ErrLimitExceeded)", len(got), err)
	}

	// Reset on a fresh stream: the budget restarts at zero.
	r.Reset(bytes.NewReader(stream))
	got, err := readAllWithSmallBuffer(t, r)
	if err != ErrLimitExceeded {
		t.Fatalf("after Reset: got %v, want ErrLimitExceeded", err)
	}
	if len(got) != 10 {
		t.Fatalf("after Reset: delivered %d bytes, want 10", len(got))
	}

	// Reset with a stream that fits within the budget succeeds fully.
	small := limitedTestStream(t, []byte("hi"))
	r.Reset(bytes.NewReader(small))
	got, err = ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("small stream: got %v, want nil", err)
	}
	if string(got) != "hi" {
		t.Fatalf("small stream: got %q, want %q", got, "hi")
	}
}

func TestLimitedReaderCloseReleasesReader(t *testing.T) {
	held := bytes.NewReader(nil)
	r := NewLimitedReader(held, 100)
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close drops the reference to the underlying reader so it can be
	// garbage collected, and further reads fail.
	if r.r != nil {
		t.Fatalf("underlying reader still referenced after Close")
	}
	if n, err := r.Read(make([]byte, 4)); n != 0 || err == nil {
		t.Fatalf("Read after Close: got (%d, %v), want non-nil error", n, err)
	}
	if _, err := r.ReadByte(); err == nil {
		t.Fatalf("ReadByte after Close: got nil error, want non-nil")
	}

	// Reset revives a closed Reader.
	r.Reset(strings.NewReader(magicChunk))
	if _, err := ioutil.ReadAll(r); err != nil {
		t.Fatalf("ReadAll after Reset: got %v, want nil", err)
	}
}

func TestLimitedReaderNegativeLimitUnlimited(t *testing.T) {
	src := bytes.Repeat([]byte{'q'}, 5000)
	stream := limitedTestStream(t, src)
	r := NewLimitedReader(bytes.NewReader(stream), -1)
	got, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("output mismatch: got %d bytes, want %d", len(got), len(src))
	}
}

func TestNewReaderUnaffectedByBudget(t *testing.T) {
	// A plain NewReader must behave exactly as before: no output budget, even
	// for streams that would exceed any reasonable limit.
	src := bytes.Repeat([]byte{'m'}, 200_000)
	stream := limitedTestStream(t, src)
	got, err := ioutil.ReadAll(NewReader(bytes.NewReader(stream)))
	if err != nil {
		t.Fatalf("got %v, want nil", err)
	}
	if !bytes.Equal(got, src) {
		t.Fatalf("output mismatch: got %d bytes, want %d", len(got), len(src))
	}
}
