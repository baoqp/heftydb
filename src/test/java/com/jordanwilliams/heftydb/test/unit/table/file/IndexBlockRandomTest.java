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

package com.jordanwilliams.heftydb.test.unit.table.file;

import com.jordanwilliams.heftydb.record.Record;
import com.jordanwilliams.heftydb.table.file.IndexBlock;
import com.jordanwilliams.heftydb.table.file.IndexRecord;
import com.jordanwilliams.heftydb.test.base.ParameterizedRecordTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IndexBlockRandomTest extends ParameterizedRecordTest {

    private final IndexBlock indexBlock;
    private final List<IndexRecord> indexRecords = new ArrayList<IndexRecord>();

    public IndexBlockRandomTest(List<Record> testRecords) {
        super(testRecords);

        int count = 0;

        IndexBlock.Builder indexBlockBuilder = new IndexBlock.Builder();

        for (Record record : records) {
            IndexRecord indexRecord = new IndexRecord(record.key(), count, 128);
            indexRecords.add(indexRecord);
            indexBlockBuilder.addRecord(indexRecord);
            count++;
        }

        this.indexBlock = indexBlockBuilder.build();
    }

    @Test
    public void iteratorTest(){
        Iterator<IndexRecord> indexRecordIterator = indexRecords.iterator();
        Iterator<IndexRecord> indexBlockIterator = indexBlock.ascendingIterator();

        while (indexRecordIterator.hasNext()){
            IndexRecord recordNext = indexRecordIterator.next();
            IndexRecord blockNext = indexBlockIterator.next();
            Assert.assertEquals("Records match", recordNext, blockNext);
        }
    }
}
