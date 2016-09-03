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
package dbs_project.storage.performance;

import dbs_project.database.DatabaseFactory;
import dbs_project.storage.Row;
import dbs_project.storage.RowCursor;
import dbs_project.storage.RowMetaData;
import dbs_project.storage.StorageLayer;
import dbs_project.storage.Table;
import dbs_project.storage.Type;
import dbs_project.util.IdCursor;
import dbs_project.util.IntIteratorWrapper;
import dbs_project.util.SimpleColumn;
import dbs_project.util.SimpleRowCursor;
import dbs_project.util.Utils;

import java.util.List;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Evaluate memory efficiency of the database
 */
public class MemoryEfficiencyTest {

    public static final int ROW_BASE_COUNT = 5000;
    public static final int ITER = 2000;
    
    private Type[] types;
    private long checksum = 0;
    
    @BeforeClass
    public static void setUpClass() throws Exception {
    	Utils.redirectStreams();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    	Utils.revertStreams();
    }
    
    @Test(timeout = 300000)
    public void heapTest() throws Exception {
        Utils.getOut().println("Adding rows to a table to check memory efficency.");
        StorageLayer storage = DatabaseFactory.INSTANCE.createInstance().getStorageLayer();
        List<SimpleColumn> columns = StorageTest.generateColumns(0);
        Table table = Utils.createEmptyTableForSimpleColumns("table", columns, storage);
        for (int i = 0; i < ITER; ++i) {
            columns = StorageTest.generateColumns(ROW_BASE_COUNT);
            SimpleRowCursor rows = new SimpleRowCursor(columns);
            IdCursor ids = table.addRows(rows.getMetaData(), rows);
            // delete some rows
            ArrayIntList deleteList = new ArrayIntList();
            for (; ids.next();) {
                if (Utils.RANDOM.nextInt() % StorageTest.DELETE_ROW_FACTOR == 0) {
                    deleteList.add(ids.getId());
                }
            }
            table.deleteRows(IntIteratorWrapper.wrap(deleteList.iterator()));
        }
        
        int rows = table.getTableMetaData().getRowCount();
        
        // compute the checksum
        computeCheckSum(table);
        
        // ensure the database contains all rows
        assertEquals("Checksum did not match", 5583047273391819253L, checksum);
        assertEquals("The row count of the database did not match the expected result", 8998681, rows);
        
        printMemory();
        Utils.getOut().println("Rowcount: "+rows);
        Utils.getOut().println("Checksum: "+checksum);
        
        try {
        	// stop garbage collection
        	storage.getTables();
        } catch(Exception e) {
        	
        }
    }
    
    private void computeCheckSum(Table table) throws Exception {
        RowCursor retrievedRowsCursor = table.getRows();
        if (retrievedRowsCursor.next()) {
            RowMetaData metaData = retrievedRowsCursor.getMetaData();
            types = new Type[metaData.getColumnCount()];
            for (int index = 0; index < metaData.getColumnCount(); ++index) {
                types[index] = metaData.getColumnMetaData(index).getType();
            }
            do {
                getRowsByPrimitives(retrievedRowsCursor);
            } while (retrievedRowsCursor.next());
        }
    }
    
    private void getRowsByPrimitives(final Row row) {
        for (int i = 0; i < types.length; ++i) {
            if (row.isNull(i)) {
                continue;
            }
            // add up checksum to avoid dead code elimination
            switch (types[i]) {
                case STRING:
                    checksum += row.getString(i).hashCode();
                    break;
                case INTEGER:
                    checksum += row.getInteger(i);
                    break;
                case DOUBLE:
                    checksum += Double.doubleToLongBits(row.getDouble(i));
                    break;
                case DATE:
                    checksum += row.getDate(i).getTime();
                    break;
                case BOOLEAN:
                    checksum += row.getBoolean(i) ? 1 : 0;
                    break;
                default:
                    checksum += row.getObject(i).hashCode();
                    break;
            }
        }
    }

    private static void printMemory() {
        for (int i = 0; i < 5; ++i) {
            System.gc();
        }
        
        double memory = ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024f / 1024f);
        Utils.getOut().println("Memory footprint: " + memory + " MB");
        Utils.getOut().println();
        
        Utils.getOut().println(
        	"<measurements layer=\"storage\">\r\n" +  
        	"<measurement><name>footprint</name><scale>2000</scale><value>" + memory + "</value></measurement>\r\n" + 
        	"</measurements>"
        );
        Utils.getOut().println();
    }
}
