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

package com.jordanwilliams.heftydb.table;

import com.jordanwilliams.heftydb.data.Key;
import com.jordanwilliams.heftydb.data.Tuple;
import com.jordanwilliams.heftydb.util.CloseableIterator;

/**
 * Represents an immutable collection of sorted record in a database.
 */
public interface Table extends Iterable<Tuple>, Comparable<Table> {

    long id();

    boolean mightContain(Key key);

    Tuple get(Key key);

    CloseableIterator<Tuple> ascendingIterator(long snapshotId);

    CloseableIterator<Tuple> descendingIterator(long snapshotId);

    CloseableIterator<Tuple> ascendingIterator(Key key, long snapshotId);

    CloseableIterator<Tuple> descendingIterator(Key key, long snapshotId);

    long tupleCount();

    long size();

    int level();

    long maxSnapshotId();

    void close();

    boolean isPersistent();
}
