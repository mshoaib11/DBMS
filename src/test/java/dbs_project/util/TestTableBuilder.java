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

import dbs_project.exceptions.NoSuchTableException;
import dbs_project.exceptions.SchemaMismatchException;
import dbs_project.exceptions.TableAlreadyExistsException;
import dbs_project.index.IndexLayer;
import dbs_project.index.IndexableTable;
import dbs_project.storage.StorageLayer;
import dbs_project.storage.Table;
import org.apache.commons.collections.primitives.ArrayIntList;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates empty or filled tables from files
 */
public final class TestTableBuilder {

    private TestTableBuilder() {
        throw new AssertionError("fail.");
    }

    private static String getFileNameFromTableName(String tableName) {
        return "/data/" + tableName.toLowerCase() + ".tbl";
    }

    public static Map<String, TableCreationResult> createTablesAndAddRows(List<String> tableNames, StorageLayer storage) throws NoSuchTableException, IOException, SchemaMismatchException, TableAlreadyExistsException {
        final Map<String, TableCreationResult> result = new HashMap<String, TableCreationResult>(tableNames.size());
        for (final String tableName : tableNames) {
            final String fileName = getFileNameFromTableName(tableName);
            final List<SimpleColumn> columnDescriptors = TableInputFileReader.createSimpleColumnsFromFile(TestTableBuilder.class.getResourceAsStream(fileName), false);
            final SimpleRowCursor rows = new SimpleRowCursor(columnDescriptors);
            final Table table = Utils.createEmptyTableForSimpleColumns(tableName, columnDescriptors, storage);
            final IdCursor ids = table.addRows(rows.getMetaData(), rows);
            final ArrayIntList idList = Utils.convertIdIteratorToList(ids);
            rows.reset();
            final TableCreationResult artifacts = new TableCreationResult(table, idList, columnDescriptors);
            result.put(table.getTableMetaData().getName(), artifacts);
            ids.close();
        }
        return result;
    }

    public static Map<String, Table> createTables(List<String> tableNames, StorageLayer storage) throws TableAlreadyExistsException, IOException, NoSuchTableException {
        final Map<String, Table> result = new HashMap<String, Table>(tableNames.size());
        for (final String tableName : tableNames) {
            final String fileName = getFileNameFromTableName(tableName);
            final List<SimpleColumn> columnDescriptors = TableInputFileReader.createSimpleColumnsFromFile(TestTableBuilder.class.getResourceAsStream(fileName), true);
            final Table table = Utils.createEmptyTableForSimpleColumns(tableName, columnDescriptors, storage);
            result.put(tableName, table);
        }
        return result;
    }

    public static Map<String, IndexableTable> createTables(List<String> tableNames, IndexLayer index) throws TableAlreadyExistsException, IOException, NoSuchTableException {
        final Map<String, IndexableTable> result = new HashMap<String, IndexableTable>(tableNames.size());
        for (final String tableName : tableNames) {
            final String fileName = getFileNameFromTableName(tableName);
            final List<SimpleColumn> columnDescriptors = TableInputFileReader.createSimpleColumnsFromFile(TestTableBuilder.class.getResourceAsStream(fileName), true);
            final IndexableTable table = (IndexableTable) Utils.createEmptyTableForSimpleColumns(tableName, columnDescriptors, index);
            result.put(tableName, table);
        }
        return result;
    }


    public static List<SimpleColumn> createSimpleColumnList(String tableName, StorageLayer storage) throws TableAlreadyExistsException, IOException, NoSuchTableException {
        final String fileName = getFileNameFromTableName(tableName);
        return TableInputFileReader.createSimpleColumnsFromFile(TestTableBuilder.class.getResourceAsStream(fileName), false);


    }
}
