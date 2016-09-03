/*
 * Copyright(c) 2012 Saarland University - Information Systems Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dbs_project.util;

import dbs_project.storage.Table;
import org.apache.commons.collections.primitives.ArrayIntList;

import java.util.List;

/**
 * Helper data structure to combine all important information about a new table that
 * we created.
 */
public final class TableCreationResult {

    private final Table table;
    private final ArrayIntList generatedIds;
    private final List<SimpleColumn> columns;

    //
    public TableCreationResult(Table table, ArrayIntList generatedIds, List<SimpleColumn> columns) {
        this.table = table;
        this.generatedIds = generatedIds;
        this.columns = columns;
    }

    public List<SimpleColumn> getColumns() {
        return columns;
    }

    public ArrayIntList getGeneratedIds() {
        return generatedIds;
    }

    public Table getTable() {
        return table;
    }
}
