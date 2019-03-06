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

package com.jordanwilliams.heftydb.index;

import com.jordanwilliams.heftydb.io.AppendChannelFile;
import com.jordanwilliams.heftydb.io.AppendFile;
import com.jordanwilliams.heftydb.state.Paths;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Incrementally writes a B+tree database index file for a stream of sorted records.
 */
public class IndexWriter {

    private final AppendFile indexFile;
    private final int maxIndexBlockSize;
    private final List<IndexBlock.Builder> indexBlockBuilders = new ArrayList<>();

    private IndexWriter(AppendFile indexFile, int maxIndexBlockSize) {
        this.indexFile = indexFile;
        this.maxIndexBlockSize = maxIndexBlockSize;
        indexBlockBuilders.add(new IndexBlock.Builder());
    }


    public void write(IndexRecord indexRecord) throws IOException {

        Queue<IndexRecord> pendingIndexRecord = new LinkedList<>(); // 保存待写入的IndexRecord
        pendingIndexRecord.add(indexRecord);

        for (int i = 0; i < indexBlockBuilders.size(); i++) {
            if (pendingIndexRecord.isEmpty()) {
                return;
            }

            IndexBlock.Builder levelBuilder = indexBlockBuilders.get(i);

            // 先判断当前Level的BlockBuilder是否已满，如果是则先把当前block写入磁盘，并创建一个新的
            // BlockBuilder来保存要加入的IndexRecord.如果发生了Block写入，那么需要在其父Block
            // 中插入一个表示刚刚写入的Block的IndexRecord，该IndexRecord存入pendingIndexRecord队列。
            // 如果当前Level的BlockBuilder未满，则直接保存到BlockBuilder即可
            if (levelBuilder.size() >= maxIndexBlockSize) {
                IndexRecord metaRecord = writeIndexBlock(levelBuilder.build());

                IndexBlock.Builder newLevelBuilder = new IndexBlock.Builder();
                newLevelBuilder.addRecord(pendingIndexRecord.poll());
                indexBlockBuilders.set(i, newLevelBuilder);

                pendingIndexRecord.add(metaRecord);
            } else {
                levelBuilder.addRecord(pendingIndexRecord.poll());
            }
        }

        if (!pendingIndexRecord.isEmpty()) {
            IndexBlock.Builder newLevelBuilder = new IndexBlock.Builder();
            newLevelBuilder.addRecord(pendingIndexRecord.poll());
            indexBlockBuilders.add(newLevelBuilder);
        }
    }

    public void finish() throws IOException {
        Queue<IndexRecord> pendingIndexRecord = new LinkedList<IndexRecord>();

        for (int i = 0; i < indexBlockBuilders.size(); i++) {
            IndexBlock.Builder levelBuilder = indexBlockBuilders.get(i);

            if (!pendingIndexRecord.isEmpty()) {
                levelBuilder.addRecord(pendingIndexRecord.poll());
            }

            IndexRecord nextLevelRecord = writeIndexBlock(levelBuilder.build());
            pendingIndexRecord.add(nextLevelRecord);
        }

        IndexRecord rootIndexRecord = pendingIndexRecord.poll();
        indexFile.appendInt(rootIndexRecord.blockSize());
        indexFile.appendLong(rootIndexRecord.blockOffset());
        indexFile.close();
    }

    private IndexRecord writeIndexBlock(IndexBlock indexBlock) throws IOException {
        ByteBuffer indexBlockBuffer = indexBlock.memory().directBuffer();
        indexBlockBuffer.rewind();

        long indexBlockOffset = indexFile.append(indexBlockBuffer);

        IndexRecord metaIndexRecord = new IndexRecord(indexBlock.startRecord().startKey(), indexBlockOffset,
                indexBlockBuffer.capacity(), false);
        indexBlock.memory().release();
        return metaIndexRecord;
    }

    public static IndexWriter open(long tableId, Paths paths, int maxIndexBlockSize) throws IOException {
        AppendFile indexFile = AppendChannelFile.open(paths.indexPath(tableId));
        return new IndexWriter(indexFile, maxIndexBlockSize);
    }
}
