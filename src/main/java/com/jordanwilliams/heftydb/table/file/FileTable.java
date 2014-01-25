/*
 * Copyright (c) 2013. Jordan Williams
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

import com.jordanwilliams.heftydb.io.DataFile;
import com.jordanwilliams.heftydb.io.MutableDataFile;
import com.jordanwilliams.heftydb.offheap.ByteMap;
import com.jordanwilliams.heftydb.offheap.Memory;
import com.jordanwilliams.heftydb.read.VersionedRecordIterator;
import com.jordanwilliams.heftydb.record.Key;
import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.state.Paths;
import com.jordanwilliams.heftydb.table.Table;
import com.jordanwilliams.heftydb.util.Sizes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.TreeSet;

public class FileTable implements Table {

    private class TableIterator implements Iterator<Record> {

        private final boolean ascending;
        private final IterationDirection iterationDirection;
        private final Queue<Record> nextRecord = new LinkedList<Record>();
        private final Iterator<Long> blockOffsets;

        private Iterator<Record> recordIterator;
        private RecordBlock recordBlock;

        public TableIterator(Key startKey, IterationDirection iterationDirection) {
            try {
                this.iterationDirection = iterationDirection;
                this.ascending = iterationDirection.equals(IterationDirection.ASCENDING);
                this.blockOffsets = blockOffsets(startKey);

                if (startKey != null) {
                    IndexRecord indexRecord = index.get(startKey);
                    this.recordBlock = readRecordBlock(indexRecord.blockOffset(), false);
                    this.recordIterator = recordBlock.iteratorFrom(startKey, iterationDirection);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public TableIterator(IterationDirection iterationDirection) {
            this(null, iterationDirection);
        }

        @Override
        public boolean hasNext() {
            if (!nextRecord.isEmpty()) {
                return true;
            }

            if (recordIterator == null || !recordIterator.hasNext()) {
                if (!nextRecordBlock()) {
                    return false;
                }
            }

            if (!recordIterator.hasNext()) {
                return false;
            }

            nextRecord.add(recordIterator.next());
            return true;
        }

        @Override
        public Record next() {
            if (nextRecord.isEmpty()) {
                hasNext();
            }

            return nextRecord.poll();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private boolean nextRecordBlock() {
            try {
                if (recordBlock != null) {
                    recordBlock.memory().release();
                }

                if (!blockOffsets.hasNext()) {
                    return false;
                }

                long blockOffset = blockOffsets.next();
                this.recordBlock = readRecordBlock(blockOffset, false);
                this.recordIterator = recordBlock.iterator(iterationDirection);

                return true;
            } catch (IOException e) {
                recordBlock.memory().release();
                throw new RuntimeException(e);
            }
        }

        private Iterator<Long> blockOffsets(Key startKey) throws IOException {
            if (startKey == null) {
                return ascending ? recordBlockOffsets.iterator() : recordBlockOffsets.descendingIterator();
            }

            IndexRecord indexRecord = index.get(startKey);

            return ascending ? recordBlockOffsets.tailSet(indexRecord.blockOffset(), false).iterator() : recordBlockOffsets.headSet
                    (indexRecord.blockOffset(), false).descendingIterator();
        }
    }

    private final long tableId;
    private final NavigableSet<Long> recordBlockOffsets;
    private final Index index;
    private final Filter filter;
    private final MetaTable metaTable;
    private final RecordBlock.Cache recordCache;
    private final DataFile tableFile;

    private FileTable(long tableId, Paths paths, RecordBlock.Cache recordCache, IndexBlock.Cache indexCache) throws IOException {
        this.tableId = tableId;
        this.recordCache = recordCache;
        this.index = Index.open(tableId, paths, indexCache);
        this.filter = Filter.open(tableId, paths);
        this.tableFile = MutableDataFile.open(paths.tablePath(tableId));
        this.metaTable = MetaTable.open(tableId, paths);
        this.recordBlockOffsets = readRecordBlockOffsets();
    }

    @Override
    public long id() {
        return tableId;
    }

    @Override
    public boolean mightContain(Key key) {
        return filter.mightContain(key);
    }

    @Override
    public Record get(Key key) {
        try {
            IndexRecord indexRecord = index.get(key);

            if (indexRecord.blockOffset() < 0) {
                return null;
            }

            RecordBlock recordBlock = readRecordBlock(indexRecord.blockOffset());
            return recordBlock.get(key);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Record> ascendingIterator(long snapshotId) {
        return new VersionedRecordIterator(snapshotId, new TableIterator(IterationDirection.ASCENDING));
    }

    @Override
    public Iterator<Record> descendingIterator(long snapshotId) {
        return new VersionedRecordIterator(snapshotId, new TableIterator(IterationDirection.DESCENDING));
    }

    @Override
    public Iterator<Record> ascendingIterator(Key key, long snapshotId) {
        return new VersionedRecordIterator(snapshotId, new TableIterator(key, IterationDirection.ASCENDING));
    }

    @Override
    public Iterator<Record> descendingIterator(Key key, long snapshotId) {
        return new VersionedRecordIterator(snapshotId, new TableIterator(key, IterationDirection.DESCENDING));
    }

    @Override
    public long recordCount() {
        return metaTable.recordCount();
    }

    @Override
    public long size() {
        return metaTable.size();
    }

    @Override
    public int level() {
        return metaTable.level();
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Override
    public Iterator<Record> iterator() {
        return new TableIterator(IterationDirection.ASCENDING);
    }

    private RecordBlock readRecordBlock(long offset) throws IOException {
        return readRecordBlock(offset, true);
    }

    private RecordBlock readRecordBlock(long offset, boolean shouldCache) throws IOException {
        RecordBlock recordBlock = recordCache.get(tableId, offset);

        if (recordBlock == null) {
            int recordBlockSize = tableFile.readInt(offset);
            Memory recordBlockMemory = Memory.allocate(recordBlockSize);

            try {
                ByteBuffer recordBlockBuffer = recordBlockMemory.directBuffer();
                tableFile.read(recordBlockBuffer, offset + Sizes.INT_SIZE);
                recordBlockBuffer.rewind();
                recordBlock = new RecordBlock(new ByteMap(recordBlockMemory));

                if (shouldCache) {
                    recordCache.put(tableId, offset, recordBlock);
                }
            } catch (IOException e){
                recordBlockMemory.release();
                throw e;
            }
        }

        return recordBlock;
    }

    private NavigableSet<Long> readRecordBlockOffsets() throws IOException {
        NavigableSet<Long> recordBlockOffsets = new TreeSet<Long>();
        long fileOffset = tableFile.size() - Sizes.INT_SIZE;
        int offsetCount = tableFile.readInt(fileOffset);

        ByteBuffer offsetBuffer = ByteBuffer.allocate(offsetCount * Sizes.LONG_SIZE);
        tableFile.read(offsetBuffer, fileOffset - offsetBuffer.capacity());
        offsetBuffer.rewind();

        for (int i = 0; i < offsetCount; i++){
            recordBlockOffsets.add(offsetBuffer.getLong());
        }

        return recordBlockOffsets;
    }

    public static FileTable open(long tableId, Paths paths, RecordBlock.Cache recordCache, IndexBlock.Cache indexCache) throws IOException {
        return new FileTable(tableId, paths, recordCache, indexCache);
    }
}
