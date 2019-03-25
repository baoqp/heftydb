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

package com.jordanwilliams.heftydb.compact;

import com.jordanwilliams.heftydb.state.Tables;
import com.jordanwilliams.heftydb.table.Table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Wraps a set of Tables and tracks compaction status for each table.
 */
public class CompactionTables {

    private final Set<Long> alreadyCompactedTables = new HashSet<>();
    private final Tables tables;

    public CompactionTables(Tables tables) {
        this.tables = tables;
    }

    public List<Table> eligibleTables() {
        List<Table> eligibleTables = new ArrayList<>();
        tables.readLock();
        try {
            for (Table table : tables){
                if (table.isPersistent() && !alreadyCompactedTables.contains(table.id())){
                    eligibleTables.add(table);
                }
            }
        } finally {
            tables.readUnlock();
        }

        return eligibleTables;
    }

    public void markAsCompacted(Table table){
        alreadyCompactedTables.add(table.id());
    }
}
