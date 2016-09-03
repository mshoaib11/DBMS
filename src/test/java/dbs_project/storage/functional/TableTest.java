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

package dbs_project.storage.functional;

import dbs_project.database.DatabaseFactory;
import dbs_project.exceptions.ColumnAlreadyExistsException;
import dbs_project.exceptions.NoSuchColumnException;
import dbs_project.storage.*;
import dbs_project.util.*;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.log4j.Logger;
import org.junit.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Functional tests for Table
 */
public final class TableTest {

    //
    private final Logger log;
    private StorageLayer storage;
    //
    private static final int UPDATE_ROW_FACTOR = 2;
    private static final int DELETE_ROW_FACTOR = 4;
    private static final int DROP_COLUMN_FACTOR = 2;
    //

    public TableTest() {
        log = Logger.getLogger(getClass());
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    	Utils.redirectStreams();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    	Utils.revertStreams();
    }

    @Before
    public void setUp() {
        storage = DatabaseFactory.INSTANCE.createInstance().getStorageLayer();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test(timeout = 300000L)
    public void testRenameColumn() throws Exception {
        Utils.getOut().println("testRenameColumn");
        final Map<String, Table> tables = TestTableBuilder.createTables(StorageLayerTest.TABLE_NAMES, storage);
        final Table table1 = tables.get(StorageLayerTest.TABLE_NAME_1);
        final String oldColName = "colString";
        final String otherExistingColName = "colInteger";
        final String newColName = "colVarchar";
        //get the test column
        final Column testColumn = Utils.getColumnByName(table1, oldColName);
        final int testColumnId = testColumn.getMetaData().getId();
        //has the column the right name?
        assertEquals("column with wrong name returned", testColumn.getMetaData().getName(), oldColName);
        try {
            //rename to other existing name which should fail
            table1.renameColumn(testColumnId, otherExistingColName);
            fail("column should already exist!");
        } catch (ColumnAlreadyExistsException ex) {
            //expected
        }
        //rename the column to new name
        table1.renameColumn(testColumnId, newColName);
        //invariants
        //new name propagated to meta data?
        assertEquals("renamed column delivers wrong name through meta data", newColName, testColumn.getMetaData().getName());
        //id still remains the same?
        assertEquals("rename operation has changed column id", testColumnId, testColumn.getMetaData().getId());
        //we also find an up to date version when asking the table?
        assertEquals("renamed column delivers wrong name through meta data", newColName, table1.getColumn(testColumnId).getMetaData().getName());
        //label is updated?
        assertEquals("renamed column delivers wrong label through meta data", table1.getTableMetaData().getName() + "." + newColName, testColumn.getMetaData().getLabel());
        //same source table?
        assertEquals("renamed column delivers wrong source table through meta data", table1.getTableMetaData().getId(), testColumn.getMetaData().getSourceTable().getTableMetaData().getId());
        //rename back to old name works?
        table1.renameColumn(testColumnId, oldColName);
        assertEquals("re-renamed column delivers wrong name through meta data", oldColName, testColumn.getMetaData().getName());
    }
    
    @Test(timeout = 300000L)
    public void testRenameColumn2() throws Exception {
        // initialize table schema (empty schema)
        final Map<String, Type> tableSchema = new HashMap<String, Type>();
        tableSchema.put("aColumn", Type.STRING);
        
        int tableUId = storage.createTable(StorageLayerTest.TABLE_NAME_1, tableSchema);
        Table table = storage.getTable(tableUId);
        
        final Map<String, ColumnMetaData> generatedSchema = table.getTableMetaData().getTableSchema();
        assertNotNull("TableSchema must not be null", generatedSchema);
        assertEquals("Table should have 1 column", 1, generatedSchema.size());
        
        ColumnMetaData col = generatedSchema.get("aColumn");
        int oldColId = col.getId();
        assertNotNull("Table should contain aColumn column", col);
        
        // rename column
        table.renameColumn(oldColId, "bColumn");
        
        final Map<String, ColumnMetaData> generatedSchema2 = table.getTableMetaData().getTableSchema();
        assertNotNull("TableSchema must not be null", generatedSchema2);
        assertEquals("Table should have 1 column", 1, generatedSchema2.size());
        
        ColumnMetaData col2 = generatedSchema2.get("bColumn");
        assertNull("Table should not contain column aColumn anymore", generatedSchema2.get("aColumn"));
        assertNotNull("Table should contain bColumn column", col2);
        
        int noColumn = col2.getId() + 1;
        if(oldColId != col2.getId()) {
        	noColumn = oldColId;
        }
        
        try {
        	table.renameColumn(noColumn, "noColumn");
        	fail("Should not be able to rename non-existant column");
        } catch(NoSuchColumnException e) {
        	// expected
        }
    }

    @Test(timeout = 300000L)
    public void testCreateColumn() throws Exception {
        Utils.getOut().println("testCreateColumn");
        final Map<String, TableCreationResult> createResults = TestTableBuilder.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        final String otherExistingColName = "colInteger";
        final String newColName = "newColumnInteger";
        final TableCreationResult resultForTable1 = createResults.get(StorageLayerTest.TABLE_NAME_1);
        final Table table1 = resultForTable1.getTable();
        try {
            table1.createColumn(otherExistingColName, Type.STRING);
            fail("column should already exist");
        } catch (ColumnAlreadyExistsException ex) {
            //expected
        }
        final int newColumnId = table1.createColumn(newColName, Type.INTEGER);
        final Column newColumn = table1.getColumn(newColumnId);
        final ColumnMetaData columnMetaData = newColumn.getMetaData();
        //invariants
        assertEquals(newColumnId, columnMetaData.getId());
        assertEquals(newColName, columnMetaData.getName());
        assertEquals(table1.getTableMetaData().getName() + "." + newColName, columnMetaData.getLabel());
        assertEquals(table1.getTableMetaData().getRowCount(), columnMetaData.getRowCount());
        assertEquals(table1.getTableMetaData().getId(), columnMetaData.getSourceTable().getTableMetaData().getId());
        // initialization
        for (int i = 0; i < table1.getTableMetaData().getRowCount(); ++i) {
            assertTrue("New columns entries should be initialized with null values", newColumn.isNull(i));
            assertEquals("New columns entries should be initialized with null values", Type.NULL_VALUE_INTEGER, newColumn.getInteger(i));
        }
    }

    @Test(timeout = 300000L)
    public void testAddAndGetRows() throws Exception {
        Utils.getOut().println("testAddAndGetRows");
        //test addRows()
        final Map<String, TableCreationResult> tableCreatResults = TestTableBuilder.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        for (final TableCreationResult artifacts : tableCreatResults.values()) {
            final Table toTestTable = artifacts.getTable();
            final List<SimpleColumn> referenceSimpleColumnList = artifacts.getColumns();
            final ArrayIntList idList = artifacts.getGeneratedIds();
            assertEquals(idList.size(), toTestTable.getTableMetaData().getRowCount());
            //test getRows
            final RowCursor toTestCursor = toTestTable.getRows(IntIteratorWrapper.wrap(idList.iterator()));
            final RowCursor referenceCursor = new SimpleRowCursor(referenceSimpleColumnList);
            assertTrue("result is different from reference: addAndgetRows", Utils.compareRowCursors(referenceCursor, toTestCursor));
//            For debug purposes. Comment out the assertion above before use.
//            Utils.getOut().println(Utils.rowCursorToHtmlTable(toTestCursor, true));
//            Utils.getOut().println(Utils.rowCursorToHtmlTable(referenceCursor, true));
            final Map<String, Type> schema = new HashMap<String, Type>();
            for (SimpleColumn column : referenceSimpleColumnList) {
                schema.put(column.getName(), column.getType());
            }
            final int randomAddCount = Utils.RANDOM.nextInt(20) + 1;
            final RowCursor additionalRandomData = new RandomRowCursor(schema, randomAddCount);
            final IdCursor generatedIdsPartTwo = toTestTable.addRows(additionalRandomData.getMetaData(), additionalRandomData);
            int idCount = 0;
            while (generatedIdsPartTwo.next()) {
                generatedIdsPartTwo.getId();
                ++idCount;
            }
            assertEquals("size of IdCursor returned by addRows() does not match the size of RowCursor", randomAddCount, idCount);

            generatedIdsPartTwo.close();
            additionalRandomData.close();
            toTestCursor.close();
            referenceCursor.close();
        }
    }

    @Test(timeout = 300000L)
    public void testUpdateRows() throws Exception {
        Utils.getOut().println("testUpdateRows");
        final Map<String, TableCreationResult> tableCreateResults = TestTableBuilder.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        for (final TableCreationResult artifacts : tableCreateResults.values()) {
            final Table toTestTable = artifacts.getTable();
            final List<SimpleColumn> referenceSimpleColumnList = artifacts.getColumns();
            final ArrayIntList idList = artifacts.getGeneratedIds();
            //choose rows for update
            final ArrayIntList updateListReference = new ArrayIntList();
            final ArrayIntList updateListToTest = new ArrayIntList();
            for (int i = 0; i < idList.size(); ++i) {
                if (Utils.RANDOM.nextInt() % UPDATE_ROW_FACTOR == 0) {
                    updateListReference.add(i);
                    updateListToTest.add(idList.get(i));
                }
            }

            final List<SimpleColumn> updateColumns = new ArrayList<SimpleColumn>(referenceSimpleColumnList.size());
            for (final SimpleColumn referenceColumn : referenceSimpleColumnList) {
                final SimpleColumn generatedRandomUpdateColumn = new SimpleColumn(updateListToTest.size(), referenceColumn.getId(), referenceColumn.getName(), referenceColumn.getType());
                for (int i = 0; i < generatedRandomUpdateColumn.getRowCount(); ++i) {
                    referenceColumn.set(updateListReference.get(i), generatedRandomUpdateColumn.getObject(i));
                }
                updateColumns.add(generatedRandomUpdateColumn);
            }
            //perform update of chosen rows
            final SimpleRowCursor updateCursor = new SimpleRowCursor(updateColumns);
            toTestTable.updateRows(updateCursor.getMetaData(), IntIteratorWrapper.wrap(updateListToTest.iterator()), updateCursor);
            final RowCursor toTestCursor = toTestTable.getRows(IntIteratorWrapper.wrap(idList.iterator()));
            final RowCursor referenceCursor = new SimpleRowCursor(referenceSimpleColumnList);
            assertTrue("result is different from reference: updateRows", Utils.compareRowCursors(referenceCursor, toTestCursor));
//            For debug purposes. Comment out the assertion above before use.
//            Utils.getOut().println(Utils.rowCursorToHtmlTable(toTestCursor, true));
//            Utils.getOut().println(Utils.rowCursorToHtmlTable(referenceCursor, true));
            toTestCursor.close();
            referenceCursor.close();
        }
    }

    @Test(timeout = 300000L)
    public void testDeleteRows() throws Exception {
        Utils.getOut().println("testDeleteRows");
        final Map<String, TableCreationResult> tableCreateResults = TestTableBuilder.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        for (final TableCreationResult artifacts : tableCreateResults.values()) {
            final Table toTestTable = artifacts.getTable();
            final List<SimpleColumn> referenceSimpleColumnList = artifacts.getColumns();
            final ArrayIntList idList = artifacts.getGeneratedIds();
            final ArrayIntList deleteReferenceList = new ArrayIntList();
            final ArrayIntList deleteTotestList = new ArrayIntList();
            //delete list backwards, so that indexes remain stable
            for (int i = idList.size(); --i >= 0; ) {
                if (Utils.RANDOM.nextInt() % DELETE_ROW_FACTOR == 0) {
                    deleteReferenceList.add(i);
                    deleteTotestList.add(idList.get(i));
                    idList.removeElementAt(i);
                }
            }
            for (final SimpleColumn referenceColumn : referenceSimpleColumnList) {
                for (int i = 0; i < deleteTotestList.size(); ++i) {
                    referenceColumn.remove(deleteReferenceList.get(i));
                }
            }
            toTestTable.deleteRows(IntIteratorWrapper.wrap(deleteTotestList.iterator()));
            final RowCursor toTestCursor = toTestTable.getRows(IntIteratorWrapper.wrap(idList.iterator()));
            final RowCursor referenceCursor = new SimpleRowCursor(referenceSimpleColumnList);
            assertTrue("result is different from reference: deleteRows", Utils.compareRowCursors(referenceCursor, toTestCursor));
//            For debug purposes. Comment out the assertion above before use.
//            Utils.getOut().println(Utils.rowCursorToHtmlTable(toTestCursor, true));
//            Utils.getOut().println(Utils.rowCursorToHtmlTable(referenceCursor, true));
            toTestCursor.close();
            referenceCursor.close();
        }
    }

    @Test(timeout = 300000L)
    public void testDropColumns() throws Exception {
        Utils.getOut().println("testDropColumns");
        final Map<String, TableCreationResult> tableCreationResultMap = TestTableBuilder.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        for (final TableCreationResult artifacts : tableCreationResultMap.values()) {
            final Table toTestTable = artifacts.getTable();
            final List<SimpleColumn> referenceSimpleColumnList = artifacts.getColumns();
            final Map<String, SimpleColumn> nameToSimpleColumnMap = new HashMap<String, SimpleColumn>();
            for (SimpleColumn column : referenceSimpleColumnList) {
                nameToSimpleColumnMap.put(column.getName(), column);
            }

            final List<ColumnMetaData> columnMetaDataWorkingCopy = new ArrayList<ColumnMetaData>(toTestTable.getTableMetaData().getTableSchema().values());
            final ArrayIntList columnsToDropIndexes = new ArrayIntList();
            for (int i = 0; i < columnMetaDataWorkingCopy.size(); ++i) {
                if (nameToSimpleColumnMap.size() < 2) {
                    break; // preserve at least one column
                }
                if (Utils.RANDOM.nextInt() % DROP_COLUMN_FACTOR == 0) {
                    ColumnMetaData toDelete = columnMetaDataWorkingCopy.get(i);
                    nameToSimpleColumnMap.remove(toDelete.getName());
                    columnsToDropIndexes.add(toDelete.getId());
                }
            }
            toTestTable.dropColumns(IntIteratorWrapper.wrap(columnsToDropIndexes.iterator()));
            final List<SimpleColumn> remainingColumns = new ArrayList<SimpleColumn>(nameToSimpleColumnMap.values());
            final SimpleRowCursor referenceCursor = new SimpleRowCursor(remainingColumns);
            final RowCursor toTestCursor = toTestTable.getRows(IntIteratorWrapper.wrap(artifacts.getGeneratedIds().iterator()));
            assertTrue("result is different from reference: dropColumns", Utils.compareRowCursors(referenceCursor, toTestCursor));
//            For debug purposes. Comment out the assertion above before use.
//            Utils.getOut().println(Utils.rowCursorToHtmlTable(toTestCursor, true));
//            Utils.getOut().println(Utils.rowCursorToHtmlTable(referenceCursor, true));
            toTestCursor.close();
            referenceCursor.close();
        }
    }

    @Test(timeout = 300000L)
    public void testGetColumns() throws Exception {
        /**
         * We will not test this function, because: - we did not clearly define
         * the semantic. - depending on how the semantic is, it can be rather
         * difficult to implement.
         *
         * However, you may find this function useful to improve your column
         * store implementation.
         *
         * You can implement your own test case here.
         */
    }

    @Test(timeout = 300000L)
    public void testUpdateColumns() throws Exception {
        /**
         * We will not test this function, because: - we did not clearly define
         * the semantic. - depending on how the semantic is, it can be rather
         * difficult to implement.
         *
         * However, you may find this function useful to improve your column
         * store implementation.
         *
         * You can implement your own test case here.
         */
    }

    @Test(timeout = 300000L)
    public void testAddColumns() throws Exception {
        /**
         * We will not test this function, because: - we did not clearly define
         * the semantic. - depending on how the semantic is, it can be rather
         * difficult to implement.
         *
         * However, you may find this function useful to improve your column
         * store implementation.
         *
         * You can implement your own test case here.
         */
    }
}
