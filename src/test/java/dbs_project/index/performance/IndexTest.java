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
package dbs_project.index.performance;

import dbs_project.database.DatabaseFactory;
import dbs_project.index.Index;
import dbs_project.index.IndexLayer;
import dbs_project.index.IndexType;
import dbs_project.index.IndexableTable;
import dbs_project.storage.Column;
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.Row;
import dbs_project.storage.RowCursor;
import dbs_project.storage.RowMetaData;
import dbs_project.storage.Type;
import dbs_project.util.*;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.text.NumberFormat;
import java.util.*;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class IndexTest {
	
	public static final int INDEX_SEED = 42;
	public static final TIntObjectHashMap<TIntLongHashMap> SEED_TO_CHECKSUM;

	static {
		SEED_TO_CHECKSUM = new TIntObjectHashMap<TIntLongHashMap>();
		
		TIntLongHashMap SEED_42 = new TIntLongHashMap();
		SEED_42.put(10, -4533295915043380090L);
		SEED_42.put(-10, -4533295914999273096L);
		SEED_42.put(100, -3780948013602406116L);
		SEED_42.put(-100, -3780948013147482478L);
		SEED_42.put(500, 1687918344643389484L);
		SEED_42.put(-500, 1687918346829223467L);
		
		SEED_TO_CHECKSUM.put(42, SEED_42);
	}

	public static final Comparator<Index> CMP = new Comparator<Index>() {
		
		@Override
		public int compare(Index o1, Index o2) {
			return o1.getIndexMetaInfo().getName().compareTo(o2.getIndexMetaInfo().getName());
		}
	};
	
    public static final String[] INDEX_COLUMNS = {
        "l_partkey", "l_extendedprice", "l_shipdate", "l_shipinstruct"
    };
    public static final int NUMBER_OF_QUERIES = 1000;
    public static List<String> results = new ArrayList<>();
    public static final String TABLE_NAME = "lineitem";
    public static final int UPDATE_ROW_FACTOR = 10;
    public static final int DELETE_ROW_FACTOR = 10;
    
    private final int scaleFactor;
    private final IndexType indexType;
	private Type[] types;
    private IndexLayer layer;
    private IndexableTable table;
    private ArrayIntList idList;
    private long checksum = 0;
    
    /* run management */
    private List<String> scaleResults;
    private static boolean scaleCompleted = true;

    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Arrays.asList(new Object[][]{
                    // first parameter is used for warm up
                    {  10, IndexType.HASH }, {  10, IndexType.HASH }, { 100, IndexType.HASH }, { 500, IndexType.HASH },
                    {  10, IndexType.TREE }, {  10, IndexType.TREE }, { 100, IndexType.TREE }, { 500, IndexType.TREE }
                });
    }


    public IndexTest(int scaleFactor, IndexType indexType) {
        this.scaleFactor = scaleFactor;
        this.indexType = indexType;
    }


    private /* static */ void outputTime(String testCaseName, int scale, IndexType it, long nanoTime) {
        String timeString = NumberFormat.getInstance(Locale.US).format(nanoTime / 1000d / 1000d / 1000d);
        Utils.getOut().println(testCaseName + "\tTime: " + timeString + " seconds");
        this.scaleResults.add("<measurement><name>"+ testCaseName + "</name>"
                               + "<scale>" + scale + "</scale>"
                               + "<type>" + it + "</type>"
                               + "<value>" + timeString + "</value></measurement>");
    }


    private /* static */ void printMemory(int scale, IndexType it) {
        for (int i = 0; i < 5; ++i) {
            System.gc();
        }
        float footprint = ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024f / 1024f);
        this.scaleResults.add("<measurement><name>footprint</name>"
                  + "<scale>" + scale + "</scale>"
                  + "<type>" + it + "</type>"
                  + "<value>" + footprint + "</value></measurement>");
        Utils.getOut().println("Memory footprint: " + footprint + " MB");
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    	Utils.redirectStreams();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Utils.getOut().println("<measurements layer=\"index\">");
        for (String res : results) {
            Utils.getOut().println(res);
        }
        Utils.getOut().println("</measurements>");
        Utils.getOut().println();
        Utils.revertStreams();
    }

    @Before
    public void setUp() throws Exception {
    	this.scaleResults = new ArrayList<>();
    	
		Utils.RANDOM.setSeed(INDEX_SEED);
    	
        layer = DatabaseFactory.INSTANCE.createInstance().getIndexLayer();
        List<SimpleColumn> columns = TPCHData.createLineitemColumns(1, 1);
        Map<String, Type> schema = new HashMap<>();
        for (Column col : columns) {
            schema.put(col.getMetaData().getName(), col.getMetaData().getType());
        }
        int tid = layer.createTable(TABLE_NAME, schema);
        table = layer.getTable(tid);
        idList = new ArrayIntList();
    }

    @After
    public void tearDown() throws Exception {
    	if(scaleCompleted) {
    		results.addAll(this.scaleResults);
    	}
    	
        for (IndexableTable t : layer.getIndexableTables()) {
            layer.deleteTable(t.getTableMetaData().getId());
        }
    }

    @Test(timeout = 300000L)
    public void indexTest() throws Exception {
    	if(!scaleCompleted && !(scaleFactor == 10)) {
    		fail("Execution aborted because previous scale failed!");
    	} else {
    		scaleCompleted = false;
    	}
    	
        Utils.getOut().println(indexType + "-Index test for scale factor " + scaleFactor);
        bulkLoadTest();
        updateRowsTest();
        deleteRowsTest();
        insertTest();
        computeChecksum(table.getRows());
        
        pointQueriesTest();
        if (indexType == IndexType.TREE) {
            rangeQueriesTest();
        }
        
		Utils.getOut().print("Checksum: " + checksum);
		TIntLongHashMap scaleToChecksum = SEED_TO_CHECKSUM.get(INDEX_SEED);
		if (scaleToChecksum != null && scaleToChecksum.containsKey(indexType == IndexType.HASH ? scaleFactor : -scaleFactor)) {
			if (scaleToChecksum.get(indexType == IndexType.HASH ? scaleFactor : -scaleFactor) != checksum) {
				Utils.getOut().println(" did not match!");
				fail("Checksums did not match!");
			}
			Utils.getOut().println(" successful!");
		} else {
			Utils.getOut().println(" could not be matched!");
		}
        
        printMemory(scaleFactor, indexType);
        Utils.getOut().println();
        
        // execution finished within time limits
        scaleCompleted = true;
    }


    private void computeChecksum(RowCursor retrievedRowsCursor) {
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


	private void bulkLoadTest() throws Exception {
        for (int i = 0; i < scaleFactor / 2; ++i) {
            List<SimpleColumn> columns = TPCHData.createLineitemColumns(scaleFactor, i);
            RowCursor rc = new SimpleRowCursor(columns);
            IdCursor ic = table.addRows(rc.getMetaData(), rc);
            while (ic.next()) {
                idList.add(ic.getId());
            }
        }
        Map<String, ColumnMetaData> schema = table.getTableMetaData().getTableSchema();

        long start = System.nanoTime();
        for (String col : INDEX_COLUMNS) {
            ColumnMetaData meta = schema.get(col);
            table.createIndex(meta.getName(), meta.getId(), indexType);
        }
        outputTime("bulkLoadTest", scaleFactor, indexType, System.nanoTime() - start);
    }


    private void updateRowsTest() throws Exception {
        long time = 0;
        int count = 0;
        for (int i = 0; i < scaleFactor; ++i) {
            ArrayIntList updateList = new ArrayIntList();
            SimpleRowCursor rc = new SimpleRowCursor(TPCHData.createLineitemColumns(scaleFactor, i));

            for (int j = 0; j < rc.getRowCount(); ++j) {
                if (Utils.RANDOM.nextInt(UPDATE_ROW_FACTOR) == 0) {
                    ++count;
                    updateList.add(idList.get(j));
                }
            }
            rc.setRowCount(updateList.size());
            long start = System.nanoTime();
            table.updateRows(rc.getMetaData(), new IntIteratorWrapper(updateList.iterator()), rc);
            time += System.nanoTime() - start;
        }
        outputTime("updateRowsTest", scaleFactor, indexType, time);
    }


    private void deleteRowsTest() throws Exception {
        ArrayIntList deleteList = new ArrayIntList();

        for (int i = 0; i < idList.size(); ++i) {
            if (Utils.RANDOM.nextInt(DELETE_ROW_FACTOR) == 0) {
                deleteList.add(idList.get(i));
            }
        }
        long start = System.nanoTime();
        table.deleteRows(new IntIteratorWrapper(deleteList.iterator()));
        outputTime("deleteRowsTest", scaleFactor, indexType, System.nanoTime() - start);
    }


    private void insertTest() throws Exception {
        long time = 0;
        for (int i = scaleFactor / 2; i < scaleFactor; ++i) {
            List<SimpleColumn> columns = TPCHData.createLineitemColumns(scaleFactor, i);
            long start = System.nanoTime();
            RowCursor rc = new SimpleRowCursor(columns);
            IdCursor ic = table.addRows(rc.getMetaData(), rc);
            time += System.nanoTime() - start;
            while (ic.next()) {
                idList.add(ic.getId());
            }
        }
        outputTime("insertTest", scaleFactor, indexType, time);
    }

    private void pointQueriesTest() throws Exception {
        long time = 0;
        
        // sort
        List<Index> indexes = new ArrayList<>(table.getIndexes());
        Collections.sort(indexes, CMP);
        
        for (Index index : indexes) {
            SimpleColumn col = null;
            switch (index.getIndexMetaInfo().getKeyColumn().getMetaData().getType()) {
                case INTEGER:
                    col = TPCHData.createForeignKeyColumn(
                            0, "l_partkey", NUMBER_OF_QUERIES, scaleFactor * TPCHData.PART_BASE_SIZE);
                    break;
                case DOUBLE:
                    col = TPCHData.createDoubleColumn(
                            0, "l_extendedprice", NUMBER_OF_QUERIES, 1, 100000);
                    break;
                case DATE:
                    col = TPCHData.createDateColumn(
                            0, "l_shipdate", NUMBER_OF_QUERIES, 694224000, 915148800);
                    break;
                case STRING:
                    col = TPCHData.createWordsColumn(
                            0, "l_shipinstruct", NUMBER_OF_QUERIES, TPCHData.INSTRUCT, 1, 1);
                    break;
            }
            
            long start = System.nanoTime();
            for (int i = 0; i < NUMBER_OF_QUERIES; i++) {            	
                iterateIds(index.pointQueryRowIds((Comparable) col.getObject(i)));   
            }   
            time += System.nanoTime() - start;
        }
        outputTime("pointQueriesTest", scaleFactor, indexType, time);
    }


    public void rangeQueriesTest() throws Exception {
        long time = 0;
        
        // sort
        List<Index> indexes = new ArrayList<>(table.getIndexes());
        Collections.sort(indexes, CMP);
        
        for (Index index : indexes) {
            SimpleColumn col = null;
            switch (index.getIndexMetaInfo().getKeyColumn().getMetaData().getType()) {
                case INTEGER:
                    col = TPCHData.createForeignKeyColumn(
                            0, "l_partkey", NUMBER_OF_QUERIES, scaleFactor * TPCHData.PART_BASE_SIZE);
                    break;
                case DOUBLE:
                    col = TPCHData.createDoubleColumn(
                            0, "l_extendedprice", NUMBER_OF_QUERIES, 1, 100000);
                    break;
                case DATE:
                    col = TPCHData.createDateColumn(
                            0, "l_shipdate", NUMBER_OF_QUERIES, 694224000, 915148800);
                    break;
                case STRING:
                    col = TPCHData.createWordsColumn(
                            0, "l_shipinstruct", NUMBER_OF_QUERIES, TPCHData.INSTRUCT, 1, 1);
                    break;
            }
            long start = System.nanoTime();
            for (int i = 0; i < NUMBER_OF_QUERIES; i += 2) {
                IdCursor ids;
                Comparable date1 = (Comparable) col.getObject(i);
                Comparable date2 = (Comparable) col.getObject(i + 1);
                if (date1.compareTo(date2) < 0) {
                    ids = index.rangeQueryRowIds(date1, date2, true, true);
                } else {
                    ids = index.rangeQueryRowIds(date2, date1, true, true);
                }
                
                iterateIds(ids);
            }
            time += System.nanoTime() - start;
            
            //System.out.println(index.getIndexMetaInfo().getKeyColumn().getMetaData().getName() + ": " + (System.nanoTime() - start));
        }
        outputTime("rangeQueriesTest", scaleFactor, indexType, time);
    }


    private void iterateIds(IdCursor ids) {
    	while (ids.next()) {
        	checksum += 1;
        }
    }
}
