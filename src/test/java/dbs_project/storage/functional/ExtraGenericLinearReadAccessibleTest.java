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
public final class ExtraGenericLinearReadAccessibleTest {

    private final Logger log;
    private StorageLayer storage;

    public ExtraGenericLinearReadAccessibleTest() {
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
    public void testBoundsRow() throws Exception {
        Utils.getOut().println("testCastsRow");
        final Map<String, TableCreationResult> createdTables = TestTableBuilder.createTablesAndAddRows(StorageLayerTest.TABLE_NAMES, storage);
        for (final TableCreationResult artifacts : createdTables.values()) {
            final Table testTable = artifacts.getTable();
            final RowCursor rowCursor = testTable.getRows();
            testForRowCursor(rowCursor);
            rowCursor.close();
        }
    }

    private int nextId(int id) {
    	return id + Utils.RANDOM.nextInt(100);
    }
    private void checkIndex(GenericLinearReadAccessible accessible, int maxIndex, Type type) {
    	try {
        	accessible.isNull(nextId(maxIndex));        
        	fail("Expected IndexOutOfBounds exception");
        } catch(IndexOutOfBoundsException e) {}
        
        try {
        	accessible.getString(nextId(maxIndex));
        } catch(IndexOutOfBoundsException e) {}
        
        try {     
        	accessible.getObject(nextId(maxIndex));
        } catch(IndexOutOfBoundsException e) {}
        
        switch (type) {
            case INTEGER:
                try {
                	accessible.getInteger(nextId(maxIndex));
                } catch(IndexOutOfBoundsException e) {}
                break;
            case DOUBLE:
                try {
                	accessible.getDouble(nextId(maxIndex));
                } catch(IndexOutOfBoundsException e) {}
                break;
            case BOOLEAN:
                try {
                	accessible.getBoolean(nextId(maxIndex));
                } catch(IndexOutOfBoundsException e) {}
                break;
            case DATE:
                try {
                	accessible.getDate(nextId(maxIndex));
                } catch(IndexOutOfBoundsException e) {}
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
                    checkIndex(rowCursor, metaData.getColumnCount(), rowTypes[i]);
                }
            } while(rowCursor.next());
        }
    }

    private void testForColumnCursor(ColumnCursor columnCursor) {
        while (columnCursor.next()) {
            final ColumnMetaData metaData = columnCursor.getMetaData();
            final Type type = metaData.getType();
            for (int i = 0; i < metaData.getRowCount(); ++i) {
                checkIndex(columnCursor, metaData.getRowCount(), type);
            }

        }
    }
}
