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
import dbs_project.storage.*;
import dbs_project.util.TableCreationResult;
import dbs_project.util.TestTableBuilder;
import dbs_project.util.Utils;

import org.apache.log4j.Logger;
import org.junit.*;

import java.util.Date;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Functional tests for GenericLinearReadAccessible
 */
public final class GenericLinearReadAccessibleTest {

    private final Logger log;
    private StorageLayer storage;

    public GenericLinearReadAccessibleTest() {
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

    @Test(timeout = 300000L)
    public void testCastsColumn() throws Exception {
        Utils.getOut().println("testCastsColumn");
        final Map<String, TableCreationResult> createdTables = TestTableBuilder.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        for (final TableCreationResult artifacts : createdTables.values()) {
            final Table testTable = artifacts.getTable();
            final ColumnCursor columnCursor = testTable.getColumns();
            testForColumnCursor(columnCursor);
            columnCursor.close();
        }
    }

    @Test(timeout = 300000L)
    public void testCastsRow() throws Exception {
        Utils.getOut().println("testCastsRow");
        final Map<String, TableCreationResult> createdTables = TestTableBuilder.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        for (final TableCreationResult artifacts : createdTables.values()) {
            final Table testTable = artifacts.getTable();
            final RowCursor rowCursor = testTable.getRows();
            testForRowCursor(rowCursor);
            rowCursor.close();
        }
    }

    private void checkIndex(GenericLinearReadAccessible accessible, int index, Type type) {
        final boolean isNull = accessible.isNull(index);
        String getString = accessible.getString(index);
        Object getObject = accessible.getObject(index);
        if (isNull) {
            assertNull(getString);
            assertNull(getObject);
        }
        switch (type) {
            case INTEGER:
                int referenceInt = accessible.getInteger(index);
                if (isNull) {
                    assertEquals("should deliver Type.NULL_VALUE_INTEGER", Type.NULL_VALUE_INTEGER, referenceInt);

                } else {
                    assertEquals("conversion should be supported: int -> double", (double) referenceInt, accessible.getDouble(index), 1E-20d);
                    assertEquals("conversion should be supported: int -> string", String.valueOf(referenceInt), getString);
                    assertEquals("conversion should be supported: int -> object", referenceInt, getObject);
                    try {
                        accessible.getBoolean(index);
                        fail("class cast exception expected: : int -> boolean");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                    try {
                        accessible.getDate(index);
                        fail("class cast exception expected: int -> date");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                }

                break;
            case DOUBLE:
                double referenceDouble = accessible.getDouble(index);
                if (isNull) {
                    assertEquals("should deliver Type.NULL_VALUE_DOUBLE", Type.NULL_VALUE_DOUBLE, referenceDouble, 1E-20d);
                } else {
                    assertEquals("conversion should be supported: double -> int", (int) referenceDouble, accessible.getInteger(index));
                    assertEquals("conversion should be supported: double -> string", String.valueOf(referenceDouble), getString);
                    assertEquals("conversion should be supported: double -> object", referenceDouble, getObject);
                    try {
                        accessible.getBoolean(index);
                        fail("class cast exception expected: double -> boolean");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                    try {
                        accessible.getDate(index);
                        fail("class cast exception expected: double -> date");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                }

                break;
            case BOOLEAN:
                boolean referenceBoolean = accessible.getBoolean(index);
                if (isNull) {
                    assertEquals("should deliver Type.NULL_VALUE_BOOLEAN", Type.NULL_VALUE_BOOLEAN, referenceBoolean);
                } else {
                    assertEquals("conversion should be supported: boolean -> string", String.valueOf(referenceBoolean), getString);
                    assertEquals("conversion should be supported: boolean -> object", referenceBoolean, getObject);
                    try {
                        accessible.getInteger(index);
                        fail("class cast exception expected: boolean -> int");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                    try {
                        accessible.getDouble(index);
                        fail("class cast exception expected: boolean -> double");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                    try {
                        accessible.getDate(index);
                        fail("class cast exception expected: boolean -> date");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                }
                break;
            case DATE:
                Date referenceDate = accessible.getDate(index);
                if (isNull) {
                    assertNull(referenceDate);
                } else {
                    assertEquals("conversion should be supported: date -> string", String.valueOf(referenceDate), getString);
                    assertEquals("conversion should be supported: date -> object", referenceDate, getObject);
                    try {
                        accessible.getInteger(index);
                        fail("class cast exception expected: date -> int");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                    try {
                        accessible.getDouble(index);
                        fail("class cast exception expected: date -> double");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                    try {
                        accessible.getBoolean(index);
                        fail("class cast exception expected: date -> boolean");
                    } catch (ClassCastException ex) {
                        //expected
                    }
                }
                break;
            default:
                break;
        }
    }

    private void testForRowCursor(RowCursor rowCursor) {
        if (rowCursor.next()) {
            final RowMetaData metaData = rowCursor.getMetaData();
            final Type[] rowTypes = new Type[metaData.getColumnCount()];
            for (int i = 0; i < metaData.getColumnCount(); ++i) {
                rowTypes[i] = metaData.getColumnMetaData(i).getType();
            }
            do {
                for (int i = 0; i < metaData.getColumnCount(); ++i) {
                    checkIndex(rowCursor, i, rowTypes[i]);
                }
            } while(rowCursor.next());
        }
    }

    private void testForColumnCursor(ColumnCursor columnCursor) {
        while (columnCursor.next()) {
            final ColumnMetaData metaData = columnCursor.getMetaData();
            final Type type = metaData.getType();
            for (int i = 0; i < metaData.getRowCount(); ++i) {
                checkIndex(columnCursor, i, type);
            }

        }
    }
}
