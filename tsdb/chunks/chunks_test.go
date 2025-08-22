// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chunks

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
)

func TestReaderWithInvalidBuffer(t *testing.T) {
	t.Parallel()
	b := realByteSlice([]byte{0x81, 0x81, 0x81, 0x81, 0x81, 0x81})
	r := &Reader{bs: []ByteSlice{b}}

	_, _, err := r.ChunkOrIterable(Meta{Ref: 0})
	require.Error(t, err)
}

func TestWriterWithDefaultSegmentSize(t *testing.T) {
	t.Parallel()
	chk1, err := ChunkFromSamples([]Sample{
		sample{t: 10, f: 11},
		sample{t: 20, f: 12},
		sample{t: 30, f: 13},
	})
	require.NoError(t, err)

	chk2, err := ChunkFromSamples([]Sample{
		sample{t: 40, h: tsdbutil.GenerateTestHistogram(1)},
		sample{t: 50, h: tsdbutil.GenerateTestHistogram(2)},
		sample{t: 60, h: tsdbutil.GenerateTestHistogram(3)},
	})
	require.NoError(t, err)

	dir := t.TempDir()
	w, err := NewWriter(dir)
	require.NoError(t, err)

	err = w.WriteChunks(chk1, chk2)
	require.NoError(t, err)

	require.NoError(t, w.Close())

	d, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, d, 1, "expected only one segment to be created to hold both chunks")
}

func TestChunkFromSamplesWithST(t *testing.T) {
	// Create samples with explicit ST (source timestamp) values.
	samples := []Sample{
		sample{t: 10, f: 11, st: 5},
		sample{t: 20, f: 12, st: 15},
		sample{t: 30, f: 13, st: 25},
	}

	chk, err := ChunkFromSamples(samples)
	require.NoError(t, err)
	require.NotNil(t, chk.Chunk)

	// Verify MinTime and MaxTime.
	require.Equal(t, int64(10), chk.MinTime)
	require.Equal(t, int64(30), chk.MaxTime)

	// Iterate over the chunk and verify ST values are preserved.
	it := chk.Chunk.Iterator(nil)
	idx := 0
	for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
		require.Equal(t, chunkenc.ValFloat, vt)
		ts, v := it.At()
		st := it.AtST()
		require.Equal(t, samples[idx].ST(), st, "ST mismatch at index %d", idx)
		require.Equal(t, samples[idx].T(), ts, "T mismatch at index %d", idx)
		require.Equal(t, samples[idx].F(), v, "F mismatch at index %d", idx)
		idx++
	}
	require.NoError(t, it.Err())
	require.Equal(t, len(samples), idx, "expected all samples to be iterated")
}

func TestChunkSizeInBytes(t *testing.T) {
	chunkMetaFloat, err := ChunkFromSamples([]Sample{
		sample{t: 10, f: 11},
		sample{t: 20, f: 12},
		sample{t: 30, f: 13},
	})
	require.NoError(t, err)

	chunkMetaHistogram, err := ChunkFromSamples([]Sample{
		sample{t: 40, h: tsdbutil.GenerateTestHistogram(1)},
		sample{t: 50, h: tsdbutil.GenerateTestHistogram(2)},
		sample{t: 60, h: tsdbutil.GenerateTestHistogram(3)},
	})
	require.NoError(t, err)

	chunkMetaFloatHistogram, err := ChunkFromSamples([]Sample{
		sample{t: 70, fh: tsdbutil.GenerateTestFloatHistogram(1)},
		sample{t: 80, fh: tsdbutil.GenerateTestFloatHistogram(2)},
		sample{t: 90, fh: tsdbutil.GenerateTestFloatHistogram(3)},
	})
	require.NoError(t, err)

	dir := t.TempDir()

	w, err := NewWriter(dir, WithSegmentSize(1024)) // Use a small segment size to avoid allocating 512MB on disk for this test.
	require.NoError(t, err)

	err = w.WriteChunks(chunkMetaFloat, chunkMetaHistogram, chunkMetaFloatHistogram)
	require.NoError(t, err)

	err = w.Close()
	require.NoError(t, err)

	segmentFile := dir + "/000001"
	segmentInfo, err := os.Stat(segmentFile)
	require.NoError(t, err)
	actualSegmentFileSize := segmentInfo.Size()

	varintBuffer := make([]byte, MaxChunkLengthFieldSize)
	chunkFloatSize := EncodedSizeInBytes(chunkMetaFloat.Chunk, varintBuffer)
	chunkHistogramSize := EncodedSizeInBytes(chunkMetaHistogram.Chunk, varintBuffer)
	chunkFloatHistogramSize := EncodedSizeInBytes(chunkMetaFloatHistogram.Chunk, varintBuffer)
	expectedSegmentFileSize := SegmentHeaderSize + chunkFloatSize + chunkHistogramSize + chunkFloatHistogramSize
	require.Equal(t, expectedSegmentFileSize, actualSegmentFileSize, "segment file size in bytes does not add up based on the chunk sizes")
}

func TestWriterSegmentFileSize(t *testing.T) {
	chunkMeta1, err := ChunkFromSamples([]Sample{
		sample{t: 10, f: 11},
	})
	require.NoError(t, err)

	chunkMeta2, err := ChunkFromSamples([]Sample{
		sample{t: 20, f: 11},
	})
	require.NoError(t, err)

	writeChunksWithSegmentSize := func(dir string, segmentSize int64) error {
		writer, err := NewWriter(dir, WithSegmentSize(segmentSize))
		if err != nil {
			return err
		}
		err = writer.WriteChunks(chunkMeta1, chunkMeta2)
		if err != nil {
			return err
		}
		return writer.Close()
	}

	// First, write both chunks to a segment with a large enough size to hold both chunks.
	dir1 := t.TempDir()
	require.NoError(t, writeChunksWithSegmentSize(dir1, 100))

	files1, err := os.ReadDir(dir1)
	require.NoError(t, err)
	require.Len(t, files1, 1, "one segment file should contain both chunks")

	// Get the actual size of the created segment file.
	segmentInfo, err := os.Stat(dir1 + "/000001")
	require.NoError(t, err)
	actualSegmentFileSize := segmentInfo.Size()

	// Now, use the actual segment file size as the limit: chunks split due to overestimation.
	dir2 := t.TempDir()
	require.NoError(t, writeChunksWithSegmentSize(dir2, actualSegmentFileSize))

	files2, err := os.ReadDir(dir2)
	require.NoError(t, err)
	require.Len(t, files2, 2, "bug: there are 2 segment files instead of 1")

	// The estimation error occurs because WriteChunks always assumes MaxChunkLengthFieldSize (5 bytes)
	// for the chunk length field, but in this test, each chunk only needs 1 byte for the length field.
	estimationError := 2 * (MaxChunkLengthFieldSize - 1)

	// Finally, use the actual segment file size + estimation error as the limit: both chunks fit again.
	dir3 := t.TempDir()
	require.NoError(t, writeChunksWithSegmentSize(dir3, actualSegmentFileSize+int64(estimationError)))

	files3, err := os.ReadDir(dir3)
	require.NoError(t, err)
	require.Len(t, files3, 1, "the Writer creates a single segment file if the segment size limit is %d bytes larger than necessary", estimationError)
}
