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
import dbs_project.exceptions.NoSuchColumnException;
import dbs_project.exceptions.NoSuchTableException;
import dbs_project.exceptions.TableAlreadyExistsException;
import dbs_project.storage.*;
import dbs_project.util.Utils;

import org.apache.log4j.Logger;
import org.junit.*;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Functional tests for StorageLayer
 */
public final class StorageLayerTest {

    public static final String TABLE_NAME_1 = "table_1";
    public static final String TABLE_NAME_2 = "table_2";
    public static final String TABLE_NAME_3 = "table_3";
    public static final List<String> TABLE_NAMES = Arrays.asList(new String[]{StorageLayerTest.TABLE_NAME_1, StorageLayerTest.TABLE_NAME_2, StorageLayerTest.TABLE_NAME_3});
    //
    private static final String SAME_HASH_STRING_1 = "0-42L";
    private static final String SAME_HASH_STRING_2 = "0-43-";
    //
    private final Logger log;
    private StorageLayer storage;

    public StorageLayerTest() {
        this.log = Logger.getLogger(getClass());
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

    /**
     * Testing that two tables with the same name cannot be created
     */
    @Test(timeout = 300000L)
    public void testTableNameDuplication() {
        Utils.getOut().println("testTableNameDuplication");

        // initialize table schema
        Map<String, Type> tableSchema = new HashMap<String, Type>();
        tableSchema.put("aColumn", Type.STRING);


        // create 1 new table
        try {
            storage.createTable(TABLE_NAME_1, tableSchema);
        } catch (TableAlreadyExistsException e) {
            fail("Table creation failed: table " + TABLE_NAME_1 + " couldn't be created");
        }

        try { // create table with duplicate name
            storage.createTable(TABLE_NAME_1, tableSchema);
            fail("Should have received an exception when adding a duplicate table: table " + TABLE_NAME_1 + " already exist");

        } catch (TableAlreadyExistsException ex) {
            // this is what we want
        }

        // create 2 with different names that have same hash code
        try {
            storage.createTable(SAME_HASH_STRING_1, tableSchema);
            storage.createTable(SAME_HASH_STRING_2, tableSchema);
        } catch (TableAlreadyExistsException e) {
            fail("Table creation failed: tables with the same hash value of their (different) name are not duplicates");
        }
    }

    /**
     * test renaming of tables
     */
    @Test(timeout = 300000L)
    public void testRenameTable() {
        Utils.getOut().println("testRenameTable");

        // initialize table schema (empty schema)
        final Map<String, Type> tableSchema = new HashMap<String, Type>();
        tableSchema.put("aColumn", Type.STRING);

        int tableUId = 0;
        // create 1 new table
        try {
            tableUId = storage.createTable(TABLE_NAME_1, tableSchema);
        } catch (TableAlreadyExistsException e) {
            fail("Table " + TABLE_NAME_1 + " must not exist as the database is empty");
        }

        // rename table
        try {
            storage.renameTable(tableUId, TABLE_NAME_2);
        } catch (NoSuchTableException e) {
            fail("Table " + TABLE_NAME_1 + " must exist");
        } catch (TableAlreadyExistsException e) {
            fail("It must be possible to rename table " + TABLE_NAME_1);
        }

        // get table name
        String newTableNameCheck = null;
        try {
            newTableNameCheck = storage.getTable(tableUId).getTableMetaData().getName();
        } catch (NoSuchTableException e) {
            fail("Table " + TABLE_NAME_2 + " must exist");
        }

        assertEquals("table names not equal", TABLE_NAME_2, newTableNameCheck);

        // take back to original state
        try {
            storage.renameTable(tableUId, TABLE_NAME_1);

        } catch (NoSuchTableException ex) {
            fail("Table " + TABLE_NAME_2 + " must exist");
        } catch (TableAlreadyExistsException e) {
            fail("It must be possible to rename table " + TABLE_NAME_2);

        }
    }

    /**
     * test renaming of tables
     */
    @Test(timeout = 300000L)
    public void testRenameTableDuplication() {
        Utils.getOut().println("testRenameTable");

        // initialize table schema (empty schema)
        final Map<String, Type> tableSchema = new HashMap<String, Type>();
        tableSchema.put("aColumn", Type.STRING);

        int tableUId = 0;
        // create 1 new table
        try {
            tableUId = storage.createTable(TABLE_NAME_1, tableSchema);
            storage.createTable(TABLE_NAME_2, tableSchema);
        } catch (TableAlreadyExistsException e) {
            fail("Table " + TABLE_NAME_1 + " must not exist as the database is empty");
        }

        // rename table
        try {
            storage.renameTable(tableUId, TABLE_NAME_2);
            fail("Table " + TABLE_NAME_2 + " exists already");
        } catch (NoSuchTableException e) {
            //expected
        } catch (TableAlreadyExistsException e) {
            //expected
        }

    }

    /**
     * Test of createTable method, of class StorageLayer.
     */
    @Test(timeout = 300000L)
    public void testCreateTable() throws Exception {
        Utils.getOut().println("testCreateTable");
        // initialize table schema
        final Map<String, Type> tableSchemaMap = new HashMap<String, Type>();
        tableSchemaMap.put("colInteger", Type.INTEGER);
        tableSchemaMap.put("colDouble", Type.DOUBLE);
        tableSchemaMap.put("colBoolean", Type.BOOLEAN);
        tableSchemaMap.put("colString", Type.STRING);
        tableSchemaMap.put("colDate", Type.DATE);
        tableSchemaMap.put("colInteger2", Type.INTEGER);
        tableSchemaMap.put("colDouble2", Type.DOUBLE);
        tableSchemaMap.put("colBoolean2", Type.BOOLEAN);
        tableSchemaMap.put("colString2", Type.STRING);
        tableSchemaMap.put("colDate2", Type.DATE);
        tableSchemaMap.put(SAME_HASH_STRING_1, Type.INTEGER);
        tableSchemaMap.put(SAME_HASH_STRING_2, Type.INTEGER);

        // create 1 new table
        final int tableUId = storage.createTable(TABLE_NAME_1, tableSchemaMap);
        //check invariants
        final Table createdTable = checkTableSchemaInvariants(tableUId, TABLE_NAME_1, tableSchemaMap);
        final RowCursor allRows = createdTable.getRows();
        //should be empty, nothing inserted yet
        assertEquals("empty table has row count != 0", 0, createdTable.getTableMetaData().getRowCount());
        assertFalse("emtpy table delivered rows", allRows.next());
        //check create table with empty schema, which should be supported
        storage.createTable(TABLE_NAME_2, new HashMap<String, Type>());
        allRows.close();
    }
    
   
    	
    private Table checkTableSchemaInvariants(int tableId, String tableName, Map<String, Type> schema) throws NoSuchTableException, IOException {
        //find right table
        final Table table = storage.getTable(tableId);
        final TableMetaData metaData = table.getTableMetaData();
        //retrieved table has right id?
        assertEquals("table id inconsistent", metaData.getId(), tableId);
        //has right name?
        assertEquals("table name inconsistent", tableName, metaData.getName());
        //has right column count according to schema?
        assertEquals("column count inconsistent", metaData.getTableSchema().size(), schema.size());
        //delivers right columns?
        int columnCount = 0;
        final int rowCount = metaData.getRowCount();
        final ColumnCursor allColumns = table.getColumns();
        final Map<String, ColumnMetaData> tableSchema = metaData.getTableSchema();
        while (allColumns.next()) {
            ++columnCount;
            final ColumnMetaData currentColMeta = allColumns.getMetaData();
            final String columnName = currentColMeta.getName();
            //expected type?
            assertEquals("type inconsistent", schema.get(columnName), currentColMeta.getType());
            //expected source table?
            assertEquals("tableId inconsistent", tableId, currentColMeta.getSourceTable().getTableMetaData().getId());
            //expected column label?
            assertEquals("table name inconsistent", table.getTableMetaData().getName() + "." + columnName, currentColMeta.getLabel());
            //
            assertTrue("table schema inconsistent", isColumnMetaDataEqual(currentColMeta, tableSchema.get(columnName)));
            //has column the right row count for this table?
            if (rowCount >= 0) {
                assertEquals("column's row count inconsistent with table", rowCount, currentColMeta.getRowCount());
            }
        }
        //delivered right amount of columns?
        assertEquals("column count inconsistent", schema.size(), columnCount);
        allColumns.close();
        return table;
    }

    private boolean isColumnMetaDataEqual(ColumnMetaData c1, ColumnMetaData c2) {
        return c1 == c2
                || (c1.getId() == c2.getId()
                && c1.getRowCount() == c2.getRowCount()
                && c1.getType().equals(c2.getType())
                && c1.getName().equals(c2.getName())
                && c1.getLabel().equals(c2.getLabel())
                && Utils.areObjectsEqual(c1.getSourceTable(), c2.getSourceTable()));
    }

    /**
     * Test of deleteTable method, of class StorageLayer.
     */
    @Test(timeout = 300000L)
    public void testDeleteTable() throws Exception {
        Utils.getOut().println("testDeleteTable");

        // initialize table schema (empty schema)
        final Map<String, Type> tableSchema = new HashMap<String, Type>();
        tableSchema.put("col1", Type.STRING);
        tableSchema.put("col2", Type.DATE);

        int tableUId = 0;

        // create 1 new table
        try {
            tableUId = storage.createTable(TABLE_NAME_1, tableSchema);
            assertEquals("wrong number of tables in db", 1, storage.getTables().size());
        } catch (TableAlreadyExistsException e) {
            fail("Table " + TABLE_NAME_1 + " must not exist as the database is empty");
        }

        try {
            Table table = storage.getTable(tableUId);
            assertEquals("inconsistent table id", tableUId, table.getTableMetaData().getId());
        } catch (NoSuchTableException e) {
            fail("Table " + TABLE_NAME_1 + " must exist");
        }

        // delete table instance
        try {
            storage.deleteTable(tableUId);
        } catch (NoSuchTableException e) {
            fail("Table " + TABLE_NAME_1 + " should have been deleted");
        }

        try {
            storage.deleteTable(tableUId);
            fail("Should have received an exception when deleting non-existing table " + TABLE_NAME_1);
        } catch (NoSuchTableException ex) {
            //expected
        }
        assertTrue("wrong number of tables in db", storage.getTables().isEmpty());

        // create the table again
        try {
            tableUId = storage.createTable(TABLE_NAME_1, tableSchema);
            assertEquals("wrong number of tables in db", 1, storage.getTables().size());
        } catch (TableAlreadyExistsException e) {
            fail("Table " + TABLE_NAME_1 + " must not exist as it was deleted");
        }
    }

    /**
     * Test of getTables method, of class StorageLayer.
     */
    @Test(timeout = 300000L)
    public void testGetTables() throws Exception {
        Utils.getOut().println("testGetTables");
        final Map<String, Type> tableSchemaMap1 = new HashMap<String, Type>();
        tableSchemaMap1.put("colInteger", Type.INTEGER);
        tableSchemaMap1.put("colDouble", Type.DOUBLE);
        tableSchemaMap1.put("colString", Type.STRING);
        tableSchemaMap1.put("colString2", Type.STRING);
        final Map<String, Type> tableSchemaMap2 = new HashMap<String, Type>();
        tableSchemaMap2.put("colInteger", Type.INTEGER);
        tableSchemaMap2.put("colBoolean", Type.BOOLEAN);
        tableSchemaMap2.put("colDate", Type.DATE);
        final Map<String, Type> tableSchemaMap3 = new HashMap<String, Type>();
        tableSchemaMap3.put("colString", Type.STRING);
        tableSchemaMap3.put("colDate", Type.DATE);
        final int id1 = storage.createTable(TABLE_NAME_1, tableSchemaMap1);
        final int id2 = storage.createTable(TABLE_NAME_2, tableSchemaMap2);
        final int id3 = storage.createTable(TABLE_NAME_3, tableSchemaMap3);
        final Map<Integer, String> idToName = new HashMap<Integer, String>(3);
        idToName.put(id1, TABLE_NAME_1);
        idToName.put(id2, TABLE_NAME_2);
        idToName.put(id3, TABLE_NAME_3);
        final Collection<Table> tables = storage.getTables();
        assertEquals("wrong table count in db", idToName.size(), tables.size());
        for (final Table table : tables) {
            final String nameFound = idToName.get(table.getTableMetaData().getId());
            assertEquals("wrong table name found", nameFound, table.getTableMetaData().getName());
        }
    }
}
