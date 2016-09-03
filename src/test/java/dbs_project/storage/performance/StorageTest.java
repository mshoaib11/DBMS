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
import dbs_project.storage.*;
import dbs_project.util.*;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.text.NumberFormat;
import java.util.*;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

/**
 * Evaluate the performance of the database when adding a set of rows
 */
@RunWith(Parameterized.class)
public class StorageTest {

	public static final int STORAGE_SEED = 1234;
	public static final TIntObjectHashMap<TIntLongHashMap> SEED_TO_CHECKSUM;

	static {
		SEED_TO_CHECKSUM = new TIntObjectHashMap<TIntLongHashMap>();
		TIntLongHashMap SEED_1234 = new TIntLongHashMap();
		SEED_1234.put(10, 2297213368333077757L);
		SEED_1234.put(100, -3645411649509749015L);
		SEED_1234.put(1000, -5390796979830570598L);
		SEED_1234.put(10000, 6036033596475215874L);
		SEED_1234.put(20000, 765036300629954441L);
		SEED_TO_CHECKSUM.put(1234, SEED_1234);
	}

	public static final int UPDATE_ROW_FACTOR = 2;
	public static final int DELETE_ROW_FACTOR = 10;
	public static final int INSERT_ROW_FACTOR = 10;
	public static final int DROP_COLUMN_FACTOR = 1;
	public static final int ROW_BASE_COUNT = 5000;
	private ArrayIntList idList, updateList, deleteList, columnsToDropIndexes;
	private List<SimpleColumn> columns;
	private String tableName;
	private StorageLayer storage;
	private Table table;
	private Type[] types;
	private static List<String> results;
	private final int scaleFactor;
	private final int rowCount;
	private long checksum = 0;

	private List<String> scaleResults;
	private static boolean scaleCompleted;

	@Parameterized.Parameters
	public static List<Object[]> data() {
		return Arrays.asList(new Object[][] {
				// first parameter is used for warm up
				{ 100 }, { 10 }, { 100 }, { 1000 }//, { 10000 }, { 20000 } 
		});
	}

	public StorageTest(int scaleFactor) {
		this.scaleFactor = scaleFactor;
		rowCount = scaleFactor * ROW_BASE_COUNT;
	}

	@BeforeClass
	public static void setUpClass() throws Exception {
		Utils.redirectStreams();

		results = new ArrayList<>();
		scaleCompleted = true;
	}

	@AfterClass
	public static void tearDownClass() throws Exception {
		Utils.getOut().println("<measurements layer=\"storage\">");
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

		Utils.RANDOM.setSeed(STORAGE_SEED);
		// set files name
		tableName = "table_" + scaleFactor;
		Utils.getOut().println(
				"Starting the performance evaluation of your database for scale factor "
						+ scaleFactor);
		// create a storage instance
		storage = DatabaseFactory.INSTANCE.createInstance().getStorageLayer();
		// create the table
		Utils.getOut().println("   (0) Initializing the test environment...");
	}

	@After
	public void tearDown() {
		if (scaleCompleted) {
			results.addAll(this.scaleResults);
		}

		Utils.getOut().println("Checksum: " + checksum);
		Utils.getOut().println();
	}

	@Test(timeout = 300000)
	public void evaluate() throws Exception {
		if (!scaleCompleted && scaleFactor != 10) {
			fail("Execution aborted because previous scale failed!");
		} else {
			scaleCompleted = false;
		}

		evaluateAddRows();
		evaluateUpdateRows();
		evaluateGetRows();
		evaluateDeleteRows();
		evaluateInsertRows();
		evaluateDropColumns();
		evaluateGetAllRows();
		evaluateCreateColumns();

		Utils.getOut().println("   (9) Verifying checksum ...");
		TIntLongHashMap scaleToChecksum = SEED_TO_CHECKSUM.get(STORAGE_SEED);
		if (scaleToChecksum != null && scaleToChecksum.containsKey(scaleFactor)) {
			if (scaleToChecksum.get(scaleFactor) != checksum) {
				Utils.getOut().println("\t-> Failed");
				fail("Checksums did not match!");
			}
			Utils.getOut().println("\t-> Successful");
		} else {
			Utils.getOut().println("\t-> No saved checksum found!");
		}

		printMemory(scaleFactor);

		// GC!
		for (Table t : storage.getTables()) {
			checksum += t.getTableMetaData().getRowCount()
					- t.getTableMetaData().getRowCount();
		}

		// execution finished within time limits
		scaleCompleted = true;
	}

	private void evaluateAddRows() throws Exception {
		columns = generateColumns(0);
		table = Utils.createEmptyTableForSimpleColumns(tableName, columns,
				storage);

		Utils.getOut().println("   (1) Inserting " + rowCount + " rows...");
		IdCursor ids;
		long time = 0;
		idList = new ArrayIntList();
		for (int i = 0; i < scaleFactor; ++i) {
			columns = generateColumns(ROW_BASE_COUNT);
			SimpleRowCursor rows = new SimpleRowCursor(columns);
			long startTime = System.nanoTime();
			ids = table.addRows(rows.getMetaData(), rows);
			idList.addAll(Utils.convertIdIteratorToList(ids));
			time += System.nanoTime() - startTime;
		}
		outputTime("addRows", scaleFactor, time);
	}

	private void evaluateUpdateRows() throws Exception {
		Utils.getOut().println(
				"   (2) Updating approximately " + rowCount / UPDATE_ROW_FACTOR
						+ " rows...");
		long time = 0;

		for (int s = 0; s < scaleFactor; ++s) {
			// choose rows to update
			updateList = new ArrayIntList();
			for (int i = s * ROW_BASE_COUNT; i < s * ROW_BASE_COUNT
					+ idList.size() / scaleFactor; ++i) {
				if (Utils.RANDOM.nextInt() % UPDATE_ROW_FACTOR == 0) {
					updateList.add(idList.get(i));
				}
			}
			final List<SimpleColumn> updateColumns = new ArrayList<>(
					columns.size());
			for (final SimpleColumn referenceColumn : columns) {
				final SimpleColumn generatedRandomUpdateColumn = new SimpleColumn(
						updateList.size(), referenceColumn.getId(),
						referenceColumn.getName(), referenceColumn.getType());
				updateColumns.add(generatedRandomUpdateColumn);
			}
			SimpleRowCursor updateCursor = new SimpleRowCursor(updateColumns);
			long startTime = System.nanoTime();
			table.updateRows(updateCursor.getMetaData(), IntIteratorWrapper.wrap(updateList.iterator()),
					updateCursor);
			time += System.nanoTime() - startTime;
		}
		outputTime("updateRows", scaleFactor, time);
	}

	private void evaluateGetRows() throws Exception {
		Utils.getOut().println("   (3) Getting all " + rowCount + " rows...");
		long time = System.nanoTime();
		RowCursor retrievedRowsCursor = table.getRows(IntIteratorWrapper
				.wrap(idList.iterator()));
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
		outputTime("getRows", scaleFactor, System.nanoTime() - time);
	}

	private void evaluateDeleteRows() throws Exception {
		// choose the rows to delete
		deleteList = new ArrayIntList();
		// delete list backwards, so that indexes remain stable
		for (int i = idList.size(); --i >= 0;) {
			if (Utils.RANDOM.nextInt() % DELETE_ROW_FACTOR == 0) {
				deleteList.add(idList.get(i));
				// idList.removeElementAt(i); // remove this (write full scan
				// after update)
			}
		}

		Utils.getOut().println(
				"   (4) Deleting " + deleteList.size() + " rows...");
		// delete the selected rows
		long time = System.nanoTime();
		table.deleteRows(IntIteratorWrapper.wrap(deleteList.iterator()));
		outputTime("deleteRows", scaleFactor, System.nanoTime() - time);
	}

	private void evaluateInsertRows() throws Exception {
		IdCursor ids;
		long time = 0;
		Utils.getOut().println(
				"   (5) Inserting " + rowCount / INSERT_ROW_FACTOR
						+ " new rows...");

		for (int i = 0; i < scaleFactor; ++i) {
			List<SimpleColumn> insertColumns = generateColumns(ROW_BASE_COUNT
					/ INSERT_ROW_FACTOR);
			SimpleRowCursor rows_inserts = new SimpleRowCursor(insertColumns);
			long startTime = System.nanoTime();
			ids = table.addRows(rows_inserts.getMetaData(), rows_inserts);
			idList.addAll(Utils.convertIdIteratorToList(ids));
			time += System.nanoTime() - startTime;
		}
		outputTime("insertRows", scaleFactor, time);
	}

	private void evaluateDropColumns() throws Exception {
		final Map<String, SimpleColumn> nameToSimpleColumnMap = new HashMap<>();
		for (SimpleColumn column : columns) {
			nameToSimpleColumnMap.put(column.getName(), column);
		}
		final List<ColumnMetaData> columnMetaDataWorkingCopy = new ArrayList<>(
				table.getTableMetaData().getTableSchema().values());
		columnsToDropIndexes = new ArrayIntList();
		for (int i = 0; i < columnMetaDataWorkingCopy.size(); ++i) {
			ColumnMetaData toDelete = columnMetaDataWorkingCopy.get(i);
			if (toDelete.getName().equals("Integer1")
					|| toDelete.getName().equals("Double2")) {
				nameToSimpleColumnMap.remove(toDelete.getName());
				columnsToDropIndexes.add(toDelete.getId());
			}
		}

		Utils.getOut().println("   (6) Removing two columns...");
		long time = System.nanoTime();
		table.dropColumns(IntIteratorWrapper.wrap(columnsToDropIndexes
				.iterator()));
		outputTime("dropColumns", scaleFactor, System.nanoTime() - time);
	}

	private void evaluateCreateColumns() throws Exception {
		Utils.getOut().println("   (8) Adding two new columns...");
		final String colOneName = "newColumnInteger";
		final String colTwoName = "newColumnString";
		long time = System.nanoTime();
		table.createColumn(colOneName, Type.INTEGER);
		table.createColumn(colTwoName, Type.STRING);
		outputTime("createColumns", scaleFactor, System.nanoTime() - time);
	}

	private void evaluateGetAllRows() throws Exception {
		Utils.getOut().println("   (7) Getting all rows...");
		long time = System.nanoTime();
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
		outputTime("getAllRows", scaleFactor, System.nanoTime() - time);
	}

	/* Generate some columns with random content */
	public static List<SimpleColumn> generateColumns(int numRows) {
		List<SimpleColumn> res = new ArrayList<>();

		res.add(new SimpleColumn(numRows, 0, "Integer1", Type.INTEGER));
		res.add(new SimpleColumn(numRows, 1, "Double1", Type.DOUBLE));
		res.add(new SimpleColumn(numRows, 2, "Boolean1", Type.BOOLEAN));
		res.add(new SimpleColumn(numRows, 3, "String1", Type.STRING));
		res.add(new SimpleColumn(numRows, 4, "Integer4", Type.INTEGER));
		res.add(new SimpleColumn(numRows, 5, "Integer2", Type.INTEGER));
		res.add(new SimpleColumn(numRows, 6, "Double2", Type.DOUBLE));
		res.add(new SimpleColumn(numRows, 7, "Integer3", Type.INTEGER));

		return res;
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

	private/* static */void outputTime(String testCaseName, int scale,
			long nanoTime) {
		String timeString = NumberFormat.getInstance(Locale.US).format(
				nanoTime / 1000d / 1000d / 1000d);
		Utils.getOut().println("\tTime: " + timeString + " seconds");
		scaleResults.add("<measurement><name>" + testCaseName + "</name>"
				+ "<scale>" + scale + "</scale>" + "<value>" + timeString
				+ "</value></measurement>");
	}

	private/* static */void printMemory(int scale) {
		for (int i = 0; i < 5; ++i) {
			System.gc();
		}
		float footprint = ((Runtime.getRuntime().totalMemory() - Runtime
				.getRuntime().freeMemory()) / 1024f / 1024f);
		scaleResults.add("<measurement><name>footprint</name>" + "<scale>"
				+ scale + "</scale>" + "<value>" + footprint
				+ "</value></measurement>");
		Utils.getOut().println("Memory footprint: " + footprint + " MB");
	}
} // end of class