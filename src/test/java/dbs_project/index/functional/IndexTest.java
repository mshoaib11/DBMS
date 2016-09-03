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

package dbs_project.index.functional;

import dbs_project.database.DatabaseFactory;
import dbs_project.exceptions.*;
import dbs_project.index.Index;
import dbs_project.index.IndexLayer;
import dbs_project.index.IndexMetaInfo;
import dbs_project.index.IndexType;
import dbs_project.index.IndexableTable;
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.RowCursor;
import dbs_project.storage.RowMetaData;
import dbs_project.storage.Type;
import dbs_project.storage.functional.StorageLayerTest;
import dbs_project.util.*;

import java.util.*;
import java.util.Map.Entry;

import org.junit.*;

import static org.junit.Assert.*;

public class IndexTest {
    private IndexLayer layer;
    public static final String NULL = "NULL";
    public static final String INDEXNAME = "IndexA";
    public static final String TABLE_NAME_1 = "table_1";
    public static final String TABLE_NAME_2 = "table_2";
    public static final String TABLE_NAME_3 = "table_3";
    public static final List<String> TABLE_NAMES = Arrays.asList(new String[]{
            StorageLayerTest.TABLE_NAME_1, StorageLayerTest.TABLE_NAME_2,
            StorageLayerTest.TABLE_NAME_3});

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    	Utils.redirectStreams();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    	Utils.revertStreams();
    }

    @Before
    public void setUp() throws Exception {
        layer = DatabaseFactory.INSTANCE.createInstance().getIndexLayer();
    }

    @After
    public void tearDown() throws Exception {
        for (IndexableTable t : layer.getIndexableTables()) {
            layer.deleteTable(t.getTableMetaData().getId());
        }
    }

    private TreeMap<Integer, HashSet<Integer>> buildIntRefIndex(RowCursor rc,
                                                                int index) {
        TreeMap<Integer, HashSet<Integer>> help = new TreeMap<>();

        do {
            HashSet<Integer> result = help.get(rc.getInteger(index));
            if (result == null) {
                result = new HashSet<>();
            }
            result.add(rc.getMetaData().getId());
            help.put(rc.getInteger(index), result);
        } while (rc.next());
        return help;

    }

    private Map<Integer, TreeMap<?, HashSet<Integer>>> buildRefIndexes(RowCursor rc) {
        Map<Integer, TreeMap<?, HashSet<Integer>>> refindexes = new HashMap<>();
        Map<Integer, TreeMap<Boolean, HashSet<Integer>>> boolrefindexes = new HashMap<>();
        Map<Integer, TreeMap<Date, HashSet<Integer>>> daterefindexes = new HashMap<>();
        Map<Integer, TreeMap<Integer, HashSet<Integer>>> intrefindexes = new HashMap<>();
        Map<Integer, TreeMap<Double, HashSet<Integer>>> doublerefindexes = new HashMap<>();
        Map<Integer, TreeMap<String, HashSet<Integer>>> stringrefindexes = new HashMap<>();
   
        for (int index = 0; index < rc.getMetaData().getColumnCount(); index++) {
            switch (rc.getMetaData().getColumnMetaData(index).getType()) {
                case BOOLEAN: {
                    boolrefindexes.put(rc.getMetaData().getColumnMetaData(index)
                            .getId(), new TreeMap<Boolean, HashSet<Integer>>());

                }
                break;
                case DATE: {
                    daterefindexes.put(rc.getMetaData().getColumnMetaData(index)
                            .getId(), new TreeMap<Date, HashSet<Integer>>(
                            new Comparator<Date>() {

                                @Override
                                public int compare(Date o1, Date o2) {
                                    return (o1 == null ? ((o2 == null) ? 0 : -1)
                                            : ((o2 == null) ? 1 : o1.compareTo(o2)));
                                }
                            }));

                }
                break;
                case INTEGER: {
                    intrefindexes.put(rc.getMetaData().getColumnMetaData(index)
                            .getId(), new TreeMap<Integer, HashSet<Integer>>());

                    break;
                }
                case DOUBLE: {
                    doublerefindexes.put(rc.getMetaData().getColumnMetaData(index)
                            .getId(), new TreeMap<Double, HashSet<Integer>>());

                }

                break;
                case STRING: {
                    stringrefindexes.put(rc.getMetaData().getColumnMetaData(index)
                            .getId(), new TreeMap<String, HashSet<Integer>>(
                            new Comparator<String>() {

                                @Override
                                public int compare(String o1, String o2) {

                                    return (o1 == null ? ((o2 == null) ? 0 : -1)
                                            : ((o2 == null) ? 1 : o1.compareTo(o2)));
                                }
                            }));

                    break;
                }
            }
        }
        do {
            for (int index = 0; index < rc.getMetaData().getColumnCount(); index++) {
                switch (rc.getMetaData().getColumnMetaData(index).getType()) {
                    case BOOLEAN: {
                        TreeMap<Boolean, HashSet<Integer>> help = boolrefindexes
                                .get(rc.getMetaData().getColumnMetaData(index)
                                        .getId());
                        HashSet<Integer> result = help.get(rc.getBoolean(index));
                        if (result == null) {
                            result = new HashSet<>();
                        }
                        result.add(rc.getMetaData().getId());
                        help.put(rc.getBoolean(index), result);

                    }
                    break;
                    case DATE: {
                        TreeMap<Date, HashSet<Integer>> help = daterefindexes
                                .get(rc.getMetaData().getColumnMetaData(index)
                                        .getId());
                        HashSet<Integer> result = help.get(rc.getDate(index));
                        if (result == null) {
                            result = new HashSet<>();
                        }
                        result.add(rc.getMetaData().getId());
                        help.put(rc.getDate(index), result);

                    }
                    break;
                    case INTEGER: {
                        TreeMap<Integer, HashSet<Integer>> help = intrefindexes
                                .get(rc.getMetaData().getColumnMetaData(index)
                                        .getId());
                        HashSet<Integer> result = help.get(rc.getInteger(index));
                        if (result == null) {
                            result = new HashSet<>();
                        }
                        result.add(rc.getMetaData().getId());
                        help.put(rc.getInteger(index), result);

                    }
                    break;

                    case DOUBLE: {
                        TreeMap<Double, HashSet<Integer>> help = doublerefindexes
                                .get(rc.getMetaData().getColumnMetaData(index)
                                        .getId());
                        HashSet<Integer> result = help.get(rc.getDouble(index));
                        if (result == null) {
                            result = new HashSet<>();
                        }
                        result.add(rc.getMetaData().getId());
                        help.put(rc.getDouble(index), result);

                    }

                    break;
                    case STRING: {
                        TreeMap<String, HashSet<Integer>> help = stringrefindexes
                                .get(rc.getMetaData().getColumnMetaData(index)
                                        .getId());
                        HashSet<Integer> result = help.get(rc.getString(index));
                        if (result == null) {
                            result = new HashSet<>();
                        }
                        result.add(rc.getMetaData().getId());
                        help.put(rc.getString(index), result);

                        break;
                    }
                }
            }

        } while (rc.next());
        
        refindexes.putAll(stringrefindexes);
        refindexes.putAll(doublerefindexes);
        refindexes.putAll(intrefindexes);
        refindexes.putAll(daterefindexes);
        refindexes.putAll(boolrefindexes);
        return refindexes;

    }

    private void createAndCheckIndex(IndexableTable table, ColumnMetaData column, IndexType type, String name, boolean supportsRange) {
        try {
            int indexId = table.createIndex(name, column.getId(), type);
            Index index = table.getIndex(indexId);
            IndexMetaInfo meta = index.getIndexMetaInfo();
            assertEquals("Index should be empty!", 0, meta.getKeyCount());
            assertEquals("Name of key column does not match!", column.getName(), meta.getKeyColumn().getMetaData().getName());
            assertEquals("Name of table does not match!", table.getTableMetaData().getName(), meta.getTable().getTableMetaData().getName());
            assertEquals("Type of index does not match!", type, meta.getIndexType());
            if (supportsRange) {
                assertTrue("Range query should be supported for " + name + "!", meta.supportsRangeQueries());
            }
        } catch (IndexAlreadyExistsException | NoSuchColumnException | NoSuchIndexException e) {
            fail(name + " " + e);
        }
    }

    @Test(timeout = 300000L)
    public void testCreateEmptyIndex() throws Exception {
        Map<String, Type> schema = new HashMap<>();
        schema.put("colB", Type.BOOLEAN);
        schema.put("colI", Type.INTEGER);
        schema.put("colDo", Type.DOUBLE);
        schema.put("colDa", Type.DATE);
        schema.put("colS", Type.STRING);
        int tableid = 0;
        try {
            tableid = layer.createTable("Table_A", schema);
        } catch (TableAlreadyExistsException e) {
            fail("Could not create Table_A although no other table should exist!");
        }
        IndexableTable table = null;
        try {
            table = layer.getTable(tableid);
        } catch (NoSuchTableException e) {
            fail("Could not retrieve table " + tableid + "!");
        }
        Map<String, ColumnMetaData> meta = table.getTableMetaData().getTableSchema();
        int indexCount = 0;

        assertEquals("Wrong number of indexes returned!", indexCount, table.getIndexes().size());
        for (Entry<String, ColumnMetaData> entry : meta.entrySet()) {
            assertEquals("Wrong number of indexes returned!", 0, table.getIndexes(entry.getValue().getId()).size());
            createAndCheckIndex(table, entry.getValue(), IndexType.HASH, "index_" + entry.getKey() + "_hash", false);
            createAndCheckIndex(table, entry.getValue(), IndexType.TREE, "index_" + entry.getKey() + "_tree", true);
            assertEquals("Wrong number of indexes returned!", 2, table.getIndexes(entry.getValue().getId()).size());
            indexCount += 2;
        }
        assertEquals("Wrong number of indexes returned!", indexCount, table.getIndexes().size());
    }

    @Test(timeout = 300000L)
    public void testDropIndex() throws Exception {
        Map<String, Type> schema = new HashMap<>();
        schema.put("colB", Type.BOOLEAN);
        schema.put("colI", Type.INTEGER);
        schema.put("colDo", Type.DOUBLE);
        schema.put("colDa", Type.DATE);
        schema.put("colS", Type.STRING);
        int tableid = layer.createTable("Table_A", schema);

        IndexableTable table = layer.getTable(tableid);

        Map<String, ColumnMetaData> meta = table.getTableMetaData()
                .getTableSchema();
        int colIid = meta.get("colI").getId();

        int indexid = table.createIndex(INDEXNAME, colIid, IndexType.HASH);

        table.getIndex(indexid);
        table.dropIndex(indexid);
        try {
            table.getIndex(indexid);
            fail("Index was not dropped!");
        } catch (NoSuchIndexException e) {
            // thats what we want
        }
        assertEquals("Wrong number of indexes returned!", 0, table.getIndexes(colIid).size());
        assertEquals("Wrong number of indexes returned!", 0, table.getIndexes().size());

        indexid = table.createIndex(INDEXNAME, colIid, IndexType.HASH);
        table.getIndex(indexid);
        table.dropColumn(colIid);
        try {
            table.getIndex(indexid);
            fail("Index was not dropped although column was dropped!");
        } catch (NoSuchIndexException e) {
            // thats what we want
        }
        assertEquals("Wrong number of indexes returned!", 0, table.getIndexes().size());

    }

    @Test(timeout = 300000L)
    public void testDuplicateNameIndex() {
        Map<String, Type> schema = new HashMap<>();
        schema.put("colB", Type.BOOLEAN);
        schema.put("colI", Type.INTEGER);
        int tableid = 0;
        try {
            tableid = layer.createTable("Table_A", schema);
        } catch (TableAlreadyExistsException e) {
            fail("Could not create Table_A although no other table should exist!");
        }
        IndexableTable table = null;
        try {
            table = layer.getTable(tableid);
        } catch (NoSuchTableException e) {
            fail("Could not retrieve table " + tableid + "!");
        }
        Map<String, ColumnMetaData> meta = table.getTableMetaData()
                .getTableSchema();

        int colBid = meta.get("colB").getId();
        int colIid = meta.get("colI").getId();

        try {
            table.createIndex(INDEXNAME, colBid, IndexType.HASH);
        } catch (IndexAlreadyExistsException e) {
            fail(INDEXNAME + ": IndexAlreadyExistsException");
        } catch (NoSuchColumnException e) {
            fail("NoSuchColumnException " + colBid + " colB");
        }

        try {
            table.createIndex(INDEXNAME, colBid, IndexType.TREE);
            fail(INDEXNAME + " should exist!");

        } catch (IndexAlreadyExistsException e) {
            // expected
        } catch (NoSuchColumnException e) {
            fail("NoSuchColumnException " + colBid + " colB");
        }

        try {
            table.createIndex(INDEXNAME, colIid, IndexType.TREE);
            fail(INDEXNAME + " should exist!");

        } catch (IndexAlreadyExistsException e) {
            // expected
        } catch (NoSuchColumnException e) {
            fail("NoSuchColumnException " + colIid + " colI");
        }
    }

    private void compareIndexes(Index index, TreeMap<?, HashSet<Integer>> refIndex) throws Exception {
        assertEquals("Number of distinct keys was not correct!",
                refIndex.keySet().size(),
                index.getIndexMetaInfo().getKeyCount());

        for (Entry<?, HashSet<Integer>> entry : refIndex.entrySet()) {
            try (IdCursor rowids = index.pointQueryRowIds((Comparable)entry.getKey())) {
                int resultsize = 0;
                int expSize = entry.getValue().size();
                while (rowids.next()) {
                    resultsize++;
                    int rowid = rowids.getId();
                    assertTrue("Returned row has not the correct key value!",
                               entry.getValue().remove(rowid));
                }
                assertEquals("Not all rowids were returned!", expSize, resultsize);
            }
        }
    }

    private void bulkLoadIndex(IndexType it) throws Exception {
        final Map<String, TableCreationResult> tableCreateResults =
                TestTableBuilder.createTablesAndAddRows(TABLE_NAMES, layer);
        for (final TableCreationResult artifacts : tableCreateResults.values()) {
            final IndexableTable toTestTable = (IndexableTable) artifacts.getTable();
            
            RowCursor rc = toTestTable.getRows();
            rc.next();
            RowMetaData meta = rc.getMetaData();
            
            Map<Integer, TreeMap<?, HashSet<Integer>>> refindexes = buildRefIndexes(rc);
            for (int i = 0; i < meta.getColumnCount(); i++) {
                ColumnMetaData toIndexColumn = meta
                        .getColumnMetaData(i);
                int indexId = toTestTable.createIndex(it.toString()
                        + toIndexColumn.getName(), toIndexColumn.getId(), it);

                Index implindex = toTestTable.getIndex(indexId);
                compareIndexes(implindex, refindexes.get(toIndexColumn.getId()));
            }
            rc.close();
        }
    }

    private void deleteRowInIndex(IndexType it) throws Exception {
        TableCreationResult result = TestTableBuilder.createTablesAndAddRows(
                Arrays.asList(new String[]{TABLE_NAME_1}), layer).get(TABLE_NAME_1);
        IndexableTable table = (IndexableTable) result.getTable();
        ColumnMetaData c = Utils.getColumnByName(table, "colInteger").getMetaData();
        int indexid = table.createIndex("A", c.getId(), it);
        table.deleteRow(result.getGeneratedIds().get(0));
        
        RowCursor rc = table.getRows();
        rc.next();
        
        int colIndex = 0;
        for (int i = 0; i < rc.getMetaData().getColumnCount(); i++) {
            if (rc.getMetaData().getColumnMetaData(i).getId() == c.getId()) {
                colIndex = i;
                break;
            }
        }
        TreeMap<Integer, HashSet<Integer>> refindex = buildIntRefIndex(rc, colIndex);
        Index index = table.getIndex(indexid);
        compareIndexes(index, refindex);
        rc.close();
    }

    private void updateRowInIndex(IndexType it) throws Exception {
        TableCreationResult result = TestTableBuilder.createTablesAndAddRows(
                Arrays.asList(new String[]{TABLE_NAME_1}), layer).get(TABLE_NAME_1);
        List<SimpleColumn> scl =
                TestTableBuilder.createSimpleColumnList(TABLE_NAME_1, layer);
        for (SimpleColumn sc : scl) {
            if (sc.getName().equals("colInteger")) {
                for (int i = 0; i < sc.getRowCount(); i++) {
                    sc.set(i, 1337);
                }
            }
        }
        RowCursor r = new SimpleRowCursor(scl);
        r.next();
        IndexableTable table = (IndexableTable) result.getTable();
        ColumnMetaData c = Utils.getColumnByName(table, "colInteger").getMetaData();
        int indexid = table.createIndex("A", c.getId(), it);
        table.updateRow(result.getGeneratedIds().get(0), r);
        
        RowCursor rc = table.getRows();
        rc.next();
        
        int colIndex = 0;
        for (int i = 0; i < rc.getMetaData().getColumnCount(); i++) {
            if (rc.getMetaData().getColumnMetaData(i).getId() == c.getId()) {
                colIndex = i;
                break;
            }
        }
        
        TreeMap<Integer, HashSet<Integer>> refindex = buildIntRefIndex(rc, colIndex);
        Index index = table.getIndex(indexid);
        compareIndexes(index, refindex);
        r.close();
        rc.close();
    }

    @Test(timeout = 300000L)
    public void testdeleteRowInHashIndex() throws Exception {
        deleteRowInIndex(IndexType.HASH);
    }

    @Test(timeout = 300000L)
    public void testdeleteRowInTreeIndex() throws Exception {
        deleteRowInIndex(IndexType.TREE);
    }

    @Test(timeout = 300000L)
    public void testupdateRowInHashIndex() throws Exception {
        updateRowInIndex(IndexType.HASH);
    }

    @Test(timeout = 300000L)
    public void testupdateRowInTreeIndex() throws Exception {
        updateRowInIndex(IndexType.TREE);
    }

    @Test(timeout = 300000L)
    public void testBulkLoadHashIndex() throws Exception {
        bulkLoadIndex(IndexType.HASH);
    }

    @Test(timeout = 300000L)
    public void testBulkLoadTreeIndex() throws Exception {
        bulkLoadIndex(IndexType.TREE);
    }

    private void insertIndex(IndexType it) throws Exception {
        final Map<String, IndexableTable> tables = TestTableBuilder
                .createTables(TABLE_NAMES, layer);
        for (Entry<String, IndexableTable> entry : tables.entrySet()) {
            List<SimpleColumn> columns = TestTableBuilder
                    .createSimpleColumnList(entry.getKey(), layer);
            IndexableTable toTestTable = (IndexableTable) Utils.getTableByName(
                    entry.getKey(), layer);

            for (SimpleColumn sc : columns) {
                ColumnMetaData toIndexColumn = Utils.getColumnByName(
                        toTestTable, sc.getName()).getMetaData();
                toTestTable.createIndex(
                        it.toString() + toIndexColumn.getName(),
                        toIndexColumn.getId(), it);

            }
            RowCursor rc = new SimpleRowCursor(columns);

            while (rc.next()) {
                toTestTable.addRow(rc);
            }
            rc.close();
            
            rc = toTestTable.getRows();
            rc.next();
            RowMetaData meta = rc.getMetaData();
            
            Map<Integer, TreeMap<?, HashSet<Integer>>> refindexes = buildRefIndexes(rc);
            for (int i = 0; i < meta.getColumnCount(); i++) {
                ColumnMetaData toIndexColumn = meta.getColumnMetaData(i);

                Index implindex = toTestTable.getIndexes(toIndexColumn.getId())
                        .iterator().next();
                compareIndexes(implindex, refindexes.get(toIndexColumn.getId()));
            }
            rc.close();
        }
    }

    @Test(timeout = 300000L)
    public void testInsertTreeIndex() throws Exception {
        insertIndex(IndexType.TREE);
    }

    @Test(timeout = 300000L)
    public void testInsertHashIndex() throws Exception {
        insertIndex(IndexType.HASH);
    }

    @Test(timeout = 300000L)
    public void testRangeQueriesInTreeIndex() throws Exception {
        TableCreationResult result = TestTableBuilder.createTablesAndAddRows(
                Arrays.asList(new String[]{TABLE_NAME_1}), layer).get(
                TABLE_NAME_1);
        IndexableTable table = (IndexableTable) result.getTable();
        ColumnMetaData c = Utils.getColumnByName(table, "colInteger")
                .getMetaData();
        int indexid = table.createIndex("A", c.getId(), IndexType.TREE);
        
        RowCursor rc = table.getRows();
        rc.next();
        
        int index = 0;
        for (int i = 0; i < rc.getMetaData().getColumnCount(); i++) {
            if (rc.getMetaData().getColumnMetaData(i).getId() == c.getId()) {
                index = i;
                break;
            }
        }
        TreeMap<Integer, HashSet<Integer>> refindex = buildIntRefIndex(rc,
                index);
        Index i = table.getIndex(indexid);
        try {
            i.rangeQueryRowIds(500, 100, true, true);
            fail("Should be an invalid range!");
        } catch (InvalidRangeException e) {
            // expected
        }
        IdCursor rowids = i.rangeQueryRowIds(100, 500, true, true);
        HashSet<Integer> refrowids = new HashSet<>();
        for (Entry<Integer, HashSet<Integer>> entry : refindex.subMap(100,
                true, 500, true).entrySet()) {
            refrowids.addAll(entry.getValue());
        }

        int resultsize = 0;
        int expSize = refrowids.size();
        while (rowids.next()) {
            resultsize++;
            assertTrue("Wrong rowid was returned!",
                    refrowids.remove(rowids.getId()));
        }
        assertEquals("Not all rowids were returned!", expSize, resultsize);
        rc.close();
        rowids.close();
    }

//    @Test(timeout = 300000L)
//    public void testRangeQueriesInHashIndex() throws Exception {
//        TableCreationResult result = TestTableBuilder.createTablesAndAddRows(
//                Arrays.asList(new String[]{TABLE_NAME_1}), layer).get(
//                TABLE_NAME_1);
//        IndexableTable table = (IndexableTable) result.getTable();
//        ColumnMetaData c = Utils.getColumnByName(table, "colInteger")
//                .getMetaData();
//        int indexid = table.createIndex("A", c.getId(), IndexType.HASH);
//        Index i = table.getIndex(indexid);
//        try {
//            i.rangeQuery(100, 500, true, true);
//            fail("Hash index should not allow range queries!");
//        } catch (RangeQueryNotSupportedException e) {}
//    }

}
