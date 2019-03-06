/*
 * Copyright (c) 2014. Jordan Williams
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jordanwilliams.heftydb.table.file;

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.index.Index;
import com.jordanwilliams.heftydb.index.IndexBlock;
import com.jordanwilliams.heftydb.index.IndexRecord;
import com.jordanwilliams.heftydb.io.ImmutableChannelFile;
import com.jordanwilliams.heftydb.io.ImmutableFile;
import com.jordanwilliams.heftydb.metrics.CacheHitGauge;
import com.jordanwilliams.heftydb.metrics.Metrics;
import com.jordanwilliams.heftydb.offheap.SortedByteMap;
import com.jordanwilliams.heftydb.offheap.MemoryAllocator;
import com.jordanwilliams.heftydb.offheap.MemoryPointer;
import com.jordanwilliams.heftydb.read.LatestTupleIterator;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.util.CloseableIterator;
import com.jordanwilliams.heftydb.util.Sizes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Provides a read-only view on a Table file. A Table file is a doubly linked list of TupleBlocks to allow for
 * efficient iteration. These blocks make up the leaves of the B+tree provided by the Index file.
 * B+树叶子节点那一层构成的双向链表
 */
public class FileTable implements Table {

    private class AscendingBlockIterator implements Iterator<TupleBlock> {

        private final long maxOffset;
        private long fileOffset = 0;

        public AscendingBlockIterator(long startOffset) {
            this.fileOffset = startOffset;
            this.maxOffset = fileSize - TableTrailer.SIZE - Sizes.INT_SIZE;
        }

        @Override
        public boolean hasNext() {
            return fileOffset < maxOffset;
        }

        @Override
        public TupleBlock next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                int nextBlockSize = tableFile.readInt(fileOffset);
                long nextBlockOffset = fileOffset + Sizes.INT_SIZE;

                fileOffset += Sizes.INT_SIZE;
                fileOffset += nextBlockSize;
                fileOffset += Sizes.INT_SIZE;

                return readTupleBlock(nextBlockOffset, nextBlockSize);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class DescendingBlockIterator implements Iterator<TupleBlock> {

        private long fileOffset;

        public DescendingBlockIterator(long startOffset) {
            this.fileOffset = startOffset;
        }

        @Override
        public boolean hasNext() {
            return fileOffset >= 0;
        }

        @Override
        public TupleBlock next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                int nextBlockSize = tableFile.readInt(fileOffset);
                long nextBlockOffset = fileOffset - nextBlockSize;

                fileOffset -= Sizes.INT_SIZE;
                fileOffset -= nextBlockSize;
                fileOffset -= Sizes.INT_SIZE;

                return readTupleBlock(nextBlockOffset, nextBlockSize);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class AscendingIterator implements CloseableIterator<Tuple> {

        protected final Iterator<TupleBlock> recordBlockIterator;
        protected Iterator<Tuple> recordIterator;
        protected TupleBlock tupleBlock;

        private AscendingIterator(Iterator<TupleBlock> recordBlockIterator, Iterator<Tuple> startIterator,
                                  TupleBlock startTupleBlock) {
            this.recordBlockIterator = recordBlockIterator;
            this.recordIterator = startIterator;
            this.tupleBlock = startTupleBlock;
        }

        private AscendingIterator(Iterator<TupleBlock> recordBlockIterator) {
            this.recordBlockIterator = recordBlockIterator;
        }

        @Override
        public boolean hasNext() {
            try {
                if (recordIterator == null || !recordIterator.hasNext()) {
                    if (!nextRecordBlock()) {
                        return false;
                    }
                }

                if (recordIterator == null || !recordIterator.hasNext()) {
                    return false;
                }

                return true;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Tuple next() {
            if (recordIterator == null || !recordIterator.hasNext()) {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
            }

            return recordIterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            if (tupleBlock != null && !tupleBlock.memory().isFree()) {
                tupleBlock.memory().free();
            }
        }

        protected boolean nextRecordBlock() throws IOException {
            if (tupleBlock != null) {
                tupleBlock.memory().free();
            }

            if (!recordBlockIterator.hasNext()) {
                return false;
            }

            tupleBlock = recordBlockIterator.next();
            recordIterator = tupleBlock.ascendingIterator();

            return true;
        }
    }

    private class DescendingIterator extends AscendingIterator {

        private DescendingIterator(Iterator<TupleBlock> recordBlockIterator, Iterator<Tuple> startIterator,
                                   TupleBlock startTupleBlock) {
            super(recordBlockIterator, startIterator, startTupleBlock);
        }

        private DescendingIterator(Iterator<TupleBlock> recordBlockIterator) {
            super(recordBlockIterator);
        }

        @Override
        protected boolean nextRecordBlock() throws IOException {
            if (tupleBlock != null) {
                tupleBlock.memory().free();
            }

            if (!recordBlockIterator.hasNext()) {
                return false;
            }

            tupleBlock = recordBlockIterator.next();
            recordIterator = tupleBlock.descendingIterator();

            return true;
        }
    }

    private final long tableId;
    private final long fileSize;
    private final Index index;
    private final TableBloomFilter tableBloomFilter;
    private final TableTrailer trailer;
    private final TupleBlock.Cache recordCache;
    private final ImmutableFile tableFile;
    private final Metrics metrics;

    private final CacheHitGauge tableCacheHitRate;

    private FileTable(long tableId, Index index, TableBloomFilter tableBloomFilter, ImmutableFile tableFile,
                      TableTrailer trailer, TupleBlock.Cache recordCache, Metrics metrics) throws IOException {
        this.tableId = tableId;
        this.recordCache = recordCache;
        this.index = index;
        this.tableBloomFilter = tableBloomFilter;
        this.tableFile = tableFile;
        this.trailer = trailer;
        this.metrics = metrics;
        this.fileSize = tableFile.size();

        this.tableCacheHitRate = metrics.hitGauge("table.cacheHitRate");
    }

    @Override
    public boolean mightContain(Key key) {
        return tableBloomFilter.mightContain(key);
    }

    @Override
    public Tuple get(Key key) {
        try {
            IndexRecord indexRecord = index.get(key);

            if (indexRecord == null) {
                return null;
            }

            TupleBlock tupleBlock = getTupleBlock(indexRecord.blockOffset(), indexRecord.blockSize());
            Tuple read = tupleBlock.get(key);
            tupleBlock.memory().release();

            return read;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CloseableIterator<Tuple> ascendingIterator(long snapshotId) {
        return new LatestTupleIterator(snapshotId, new AscendingIterator(new AscendingBlockIterator(0)));
    }

    @Override
    public CloseableIterator<Tuple> descendingIterator(long snapshotId) {
        try {
            long startOffset = tableFile.size() - TableTrailer.SIZE - Sizes.INT_SIZE;
            return new LatestTupleIterator(snapshotId, new DescendingIterator(new DescendingBlockIterator
                    (startOffset)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CloseableIterator<Tuple> ascendingIterator(Key key, long snapshotId) {
        try {
            IndexRecord indexRecord = index.get(key);

            if (indexRecord == null) {
                return new CloseableIterator.Wrapper<Tuple>(Collections.<Tuple>emptyIterator());
            }

            TupleBlock startTupleBlock = readTupleBlock(indexRecord.blockOffset(), indexRecord.blockSize());
            Iterator<Tuple> startRecordIterator = startTupleBlock.ascendingIterator(key);
            long nextBlockOffset = indexRecord.blockOffset() + indexRecord.blockSize() + Sizes.INT_SIZE;
            return new LatestTupleIterator(snapshotId, new AscendingIterator(new AscendingBlockIterator
                    (nextBlockOffset), startRecordIterator, startTupleBlock));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CloseableIterator<Tuple> descendingIterator(Key key, long snapshotId) {
        try {
            IndexRecord indexRecord = index.get(key);

            if (indexRecord == null) {
                return new CloseableIterator.Wrapper<Tuple>(Collections.<Tuple>emptyIterator());
            }

            TupleBlock startTupleBlock = readTupleBlock(indexRecord.blockOffset(), indexRecord.blockSize());
            Iterator<Tuple> startRecordIterator = startTupleBlock.descendingIterator(key);
            long nextBlockOffset = indexRecord.blockOffset() - Sizes.LONG_SIZE;
            return new LatestTupleIterator(snapshotId, new DescendingIterator(new DescendingBlockIterator
                    (nextBlockOffset), startRecordIterator, startTupleBlock));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long id() {
        return tableId;
    }

    @Override
    public long tupleCount() {
        return trailer.recordCount();
    }

    @Override
    public long size() {
        return fileSize;
    }

    @Override
    public int level() {
        return trailer.level();
    }

    @Override
    public long maxSnapshotId() {
        return trailer.maxSnapshotId();
    }

    @Override
    public void close() {
        try {
            index.close();
            tableFile.close();
            tableBloomFilter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Override
    public Iterator<Tuple> iterator() {
        return new AscendingIterator(new AscendingBlockIterator(0));
    }

    @Override
    public int compareTo(Table o) {
        return Long.compare(tableId, o.id());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileTable fileTable = (FileTable) o;

        if (tableId != fileTable.tableId) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (tableId ^ (tableId >>> 32));
    }

    @Override
    public String toString() {
        return "FileTable{" +
                "tableId=" + tableId +
                ", level=" + trailer.level() +
                '}';
    }

    private TupleBlock getTupleBlock(long offset, int size) throws IOException {
        TupleBlock tupleBlock = recordCache.get(tableId, offset);
        tableCacheHitRate.sample(tupleBlock != null);

        if (tupleBlock == null) {
            tupleBlock = readTupleBlock(offset, size);
            recordCache.put(tableId, offset, tupleBlock);
        }

        return tupleBlock;
    }

    private TupleBlock readTupleBlock(long offset, int size) throws IOException {
        MemoryPointer recordBlockPointer = MemoryAllocator.allocate(size);

        try {
            ByteBuffer recordBlockBuffer = recordBlockPointer.directBuffer();
            tableFile.read(recordBlockBuffer, offset);
            recordBlockBuffer.rewind();
            return new TupleBlock(new SortedByteMap(recordBlockPointer));
        } catch (IOException e) {
            recordBlockPointer.release();
            throw e;
        }
    }

    public static FileTable open(long tableId, Paths paths, TupleBlock.Cache recordCache,
                                 IndexBlock.Cache indexCache, Metrics metrics) throws IOException {
        Index index = Index.open(tableId, paths, indexCache, metrics);
        TableBloomFilter tableBloomFilter = TableBloomFilter.read(tableId, paths);
        ImmutableFile tableFile = ImmutableChannelFile.open(paths.tablePath(tableId));
        TableTrailer trailer = TableTrailer.read(tableFile);
        return new FileTable(tableId, index, tableBloomFilter, tableFile, trailer, recordCache, metrics);
    }
}
