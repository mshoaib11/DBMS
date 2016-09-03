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

package dbs_project.query.performance;

import dbs_project.database.DatabaseFactory;
import dbs_project.index.IndexType;
import dbs_project.query.QueryLayer;
import dbs_project.query.functional.Statements;
import dbs_project.query.predicate.Constant;
import dbs_project.query.predicate.Expression;
import dbs_project.query.predicate.Operator;
import dbs_project.query.predicate.impl.Constants;
import dbs_project.query.predicate.impl.Expressions;
import dbs_project.query.statement.*;
import dbs_project.storage.Relation;
import dbs_project.storage.Row;
import dbs_project.storage.RowCursor;
import dbs_project.storage.RowMetaData;
import dbs_project.storage.Type;
import dbs_project.util.TPCHData;
import dbs_project.util.Utils;
import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class QueryLayerTest {
	
	private static int QUERY_SEED = 60714;
	public static final TIntObjectHashMap<TIntLongHashMap> SEED_TO_CHECKSUM;

	static {
		SEED_TO_CHECKSUM = new TIntObjectHashMap<TIntLongHashMap>();
		
		TIntLongHashMap SEED_60714 = new TIntLongHashMap();
		SEED_60714.put(10, 5419700958143535635L);
		SEED_60714.put(100, 1401114312227565857L);
		SEED_60714.put(1000, 8484811759429192497L);
		
		SEED_TO_CHECKSUM.put(60714, SEED_60714);
	}

    private final int scaleFactor;
    private QueryLayer qLayer;
    private static List<String> results;
    
    // LINEITEM
    private static final String LINEITEM = "lineitem";
    private static final List<String> litem_columns = Arrays.asList("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
            "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate",
            "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment");
    private static final List<Type> litem_types = Arrays.asList(Type.INTEGER, Type.INTEGER, Type.INTEGER,
            Type.INTEGER, Type.INTEGER, Type.DOUBLE, Type.DOUBLE, Type.DOUBLE, Type.STRING, Type.STRING, Type.DATE, Type.DATE, Type.DATE,
            Type.STRING, Type.STRING, Type.STRING);
    
    // CUSTOMER
    private static final String CUSTOMER = "customer";
    private static final List<String> cust_columns = Arrays.asList("c_custkey", "c_name", "c_address", "c_nationkey",
            "c_phone", "c_acctbal", "c_mktsegment", "c_comment");
    private static final List<Type> cust_types = Arrays.asList(Type.INTEGER, Type.STRING, Type.STRING,
            Type.INTEGER, Type.STRING, Type.DOUBLE, Type.STRING, Type.STRING);
    
    // CUSTOMER
    private static final String ORDERS = "orders";
    private static final List<String> order_columns = Arrays.asList("o_orderkey", "o_custkey", "o_orderstatus",
            "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment");
    private static final List<Type> order_types = Arrays.asList(Type.INTEGER, Type.INTEGER, Type.STRING,
            Type.DOUBLE, Type.DATE, Type.STRING, Type.STRING, Type.INTEGER, Type.STRING);
    private static final List<String> litem_columns_update = Arrays.asList("l_quantity");
    private static final List<String> litem_values_update = Arrays.asList("1");
    private static final List<String> selectAll = Arrays.asList("*");
    private static final List<String> selectAtts = Arrays.asList("l_orderkey", "o_orderstatus", "c_name");
    
	private Type[] types;
    private List<String> scaleResults;
    private static boolean scaleCompleted = true;
    
    private long checksum = 0;

    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Arrays.asList(new Object[][]{
                    // first parameter is used for warm up
                    {   10 },
                    {   10 },
                    {  100 },
                    { 1000 }
                });
    }


    public QueryLayerTest(int scaleFactor) {
        this.scaleFactor = scaleFactor;
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    	Utils.redirectStreams();
    	
    	TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    	
        results = new ArrayList<>();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        Utils.getOut().println("<measurements layer=\"query\">");
        for (String res : results) {
            Utils.getOut().println(res);
        }
        Utils.getOut().println("</measurements>");
        Utils.getOut().println();
        
        Utils.revertStreams();
    }

    @Before
    public void setUp() throws Exception {
    	this.checksum = 0;
    	this.scaleResults = new ArrayList<>();
    	
		Utils.RANDOM.setSeed(QUERY_SEED);
    	
        Utils.getOut().println("Testing performance for scale factor " + scaleFactor);
        qLayer = DatabaseFactory.INSTANCE.createInstance().getQueryLayer();
    }

    @After
    public void tearDown() throws Exception {
    	if(scaleCompleted) {
    		results.addAll(this.scaleResults);
    	}
        Utils.getOut().println();
    }

    @Test(timeout = 300000L)
    public void testPerformance() throws Exception {
    	if(!scaleCompleted) {
    		fail("Execution aborted because previous scale failed!");
    	} else {
    		scaleCompleted = false;
    	}
    	
        createTable(LINEITEM, litem_columns, litem_types);
        createTable(ORDERS, order_columns, order_types);
        createTable(CUSTOMER, cust_columns, cust_types);

        insertRows(ORDERS);
        insertRows(CUSTOMER);

        testInsertRows();
        testDeleteRows();
        testUpdateRows();

        testExecuteSelectPointQuery();
        testExecuteSelectRangeQuery();
        testExecuteComplpexSelectQuery();
        testExecuteSelectJoinQuery();
        
        for(String tableName : new String[]{ LINEITEM, ORDERS, CUSTOMER }) {
	        computeChecksum(
	        	qLayer.executeQuery(
	        		Statements.buildQueryStatement(
	        			Arrays.asList(tableName), Arrays.asList("*"), null
	        		)
	        	).getRows()
	        );
        }
        
		Utils.getOut().print("Checksum: " + checksum);
		TIntLongHashMap scaleToChecksum = SEED_TO_CHECKSUM.get(QUERY_SEED);
		if (scaleToChecksum != null && scaleToChecksum.containsKey(scaleFactor)) {
			if (scaleToChecksum.get(scaleFactor) != checksum) {
				Utils.getOut().println(" did not match!");
				fail("Checksums did not match!");
			}
			Utils.getOut().println(" was successful verified!");
		} else {
			Utils.getOut().println(" (no saved checksum found)");
		}
        
        printMemory(scaleFactor);
        
        // execution finished within time limits
        scaleCompleted = true;
    }

    /*
     * UNIT TESTS
     */
    private void testInsertRows() throws Exception {
        long time = 0;
        for (int i = 0; i < scaleFactor; i++) {
            InsertRowsStatement insertStmnt = Utils.getInsertStatement(LINEITEM,
                    TPCHData.createColumns(LINEITEM, scaleFactor, i),
                    TPCHData.getBaseSize(LINEITEM));
            long startTime = System.nanoTime();
            qLayer.executeInsertRows(insertStmnt);
            time += System.nanoTime() - startTime;
        }
        outputTime("InsertRows", scaleFactor, time);
    }


    private void testDeleteRows() throws Exception {
        // Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("l_orderkey");
        Constant idValConst = Constants.createLiteralConstant("10051");
        Expression exp = Expressions.createExpression(Operator.LT, idColumnNameConst, idValConst);
        DeleteRowsStatement st = Statements.buildDeleteRowsStatement(LINEITEM, exp);

        // start time
        long startTime = System.nanoTime();
        checksum += qLayer.executeDeleteRows(st);
        //Utils.getOut().println("testDeleteRows: " + checksum);
        // elapsed test time
        outputTime("DeleteRows", scaleFactor, System.nanoTime() - startTime);
    }


    private void testUpdateRows() throws Exception {
        // Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("l_quantity");
        Constant idValConst = Constants.createLiteralConstant("10");
        Expression exp = Expressions.createExpression(Operator.LEQ, idColumnNameConst, idValConst);
        UpdateRowsStatement st = Statements.buildUpdateRowStatement(LINEITEM, litem_columns_update, exp, litem_values_update);

        // start time
        long startTime = System.nanoTime();
        checksum += qLayer.executeUpdateRows(st);
        //Utils.getOut().println("testUpdateRows: " + checksum);
        // elapsed test time
        outputTime("UpdateRows", scaleFactor, System.nanoTime() - startTime);
    }


    private void testExecuteSelectPointQuery() throws Exception {
        // no index query 1
        Constant idColumnNameConst = Constants.createColumnNameConstant("l_orderkey");
        Constant idValConst = Constants.createLiteralConstant("800000");
        Expression exp = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        QueryStatement st = Statements.buildQueryStatement(selectAll, exp, LINEITEM);
        // start time
        long startTime = System.nanoTime();
        Relation rel = qLayer.executeQuery(st);
        iterateRelation(rel);
        // elapsed test time
        outputTime("SelectRow-NoIndexQ1", scaleFactor, System.nanoTime() - startTime);

        // no index query 2
        idValConst = Constants.createLiteralConstant("28901");
        exp = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        st = Statements.buildQueryStatement(selectAll, exp, LINEITEM);
        // start time
        startTime = System.nanoTime();
        rel = qLayer.executeQuery(st);
        iterateRelation(rel);
        // elapsed test time
        outputTime("SelectRow-NoIndexQ2", scaleFactor, System.nanoTime() - startTime);

        // create index 1
        CreateIndexStatement idxSt = Statements.buildCreateIndexStatement(LINEITEM, "l_orderkey", "litemindex", IndexType.HASH);
        qLayer.createIndex(idxSt);
        // index query 1
        idValConst = Constants.createLiteralConstant("600000");
        exp = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        st = Statements.buildQueryStatement(selectAll, exp, LINEITEM);
        // start time
        startTime = System.nanoTime();
        rel = qLayer.executeQuery(st);
        iterateRelation(rel);
        // elapsed test time
        outputTime("SelectRow-IndexQ1", scaleFactor, System.nanoTime() - startTime);
        // drop index 1
        qLayer.dropIndex(Statements.buildDropIndexStatement(LINEITEM, "l_orderkey", "litemindex"));

        // create index 2
        idxSt = Statements.buildCreateIndexStatement(LINEITEM, "l_orderkey", "litemindex", IndexType.TREE);
        qLayer.createIndex(idxSt);
        // index query 2
        idValConst = Constants.createLiteralConstant("600001"); // increment by one to avoid caching
        exp = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        st = Statements.buildQueryStatement(selectAll, exp, LINEITEM);
        // start time
        startTime = System.nanoTime();
        rel = qLayer.executeQuery(st);
        iterateRelation(rel);
        // elapsed test time
        outputTime("SelectRow-IndexQ2", scaleFactor, System.nanoTime() - startTime);

        // create again index 1
        idxSt = Statements.buildCreateIndexStatement(LINEITEM, "l_orderkey", "anotherlitemindex", IndexType.HASH);
        qLayer.createIndex(idxSt);
        // both indexes query 3: Can you pick the right index?
        idValConst = Constants.createLiteralConstant("600002"); // increment by one to avoid caching
        exp = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        st = Statements.buildQueryStatement(selectAll, exp, LINEITEM);
        // start time
        startTime = System.nanoTime();
        rel = qLayer.executeQuery(st);
        iterateRelation(rel);
        // elapsed test time
        outputTime("SelectRow-IndexQ3", scaleFactor, System.nanoTime() - startTime);

        // drop indexes
        qLayer.dropIndex(Statements.buildDropIndexStatement(LINEITEM, "l_orderkey", "litemindex"));
        qLayer.dropIndex(Statements.buildDropIndexStatement(LINEITEM, "l_orderkey", "anotherlitemindex"));
    }


    private void testExecuteSelectRangeQuery() throws Exception {
        /*
         * two queries (small and large range)
         * the same two queries with an index
         */
        // Query1: small range query
        Constant idColumnNameConst = Constants.createColumnNameConstant("l_quantity");
        Constant idValConst = Constants.createLiteralConstant("2");
        Expression exp = Expressions.createExpression(Operator.LEQ, idColumnNameConst, idValConst);
        QueryStatement q1 = Statements.buildQueryStatement(selectAll, exp, LINEITEM);
        // Query2: large range query
        idColumnNameConst = Constants.createColumnNameConstant("l_quantity");
        idValConst = Constants.createLiteralConstant("30");
        exp = Expressions.createExpression(Operator.LEQ, idColumnNameConst, idValConst);
        QueryStatement q2 = Statements.buildQueryStatement(selectAll, exp, LINEITEM);

        /*
         * No index queries
         */
        // start time
        long startTime = System.nanoTime();
        Relation rel = qLayer.executeQuery(q1);
        iterateRelation(rel);
        // elapsed test time
        outputTime("SelectRows-NoIndexQ1", scaleFactor, System.nanoTime() - startTime);
        // start time
        startTime = System.nanoTime();
        rel = qLayer.executeQuery(q2);
        iterateRelation(rel);
        // elapsed test time
        outputTime("SelectRows-NoIndexQ2", scaleFactor, System.nanoTime() - startTime);

        // create index on l_quantity
        CreateIndexStatement idx = Statements.buildCreateIndexStatement(LINEITEM, "l_quantity", "litemindex", IndexType.TREE);
        qLayer.createIndex(idx);

        /*
         * Index queries
         */
        // start time
        startTime = System.nanoTime();
        rel = qLayer.executeQuery(q1);
        iterateRelation(rel);
        // elapsed test time
        outputTime("SelectRows-IndexQ1", scaleFactor, System.nanoTime() - startTime);
        // start time
        startTime = System.nanoTime();
        rel = qLayer.executeQuery(q2);
        iterateRelation(rel);
        // elapsed test time
        outputTime("SelectRows-IndexQ2", scaleFactor, System.nanoTime() - startTime);

        // drop index
        qLayer.dropIndex(Statements.buildDropIndexStatement(LINEITEM, "l_quantity", "litemindex"));
    }


    private void testExecuteComplpexSelectQuery() throws Exception {
        // Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("l_shipmode");
        Constant idValConst = Constants.createLiteralConstant("AIR");
        Expression exp1 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        idColumnNameConst = Constants.createColumnNameConstant("l_quantity");
        idValConst = Constants.createLiteralConstant("50");
        Expression exp2 = Expressions.createExpression(Operator.LEQ, idColumnNameConst, idValConst);

        // First compound
        Expression andExp1 = Expressions.createExpression(Operator.AND, exp1, exp2);

        idColumnNameConst = Constants.createColumnNameConstant("l_shipmode");
        idValConst = Constants.createLiteralConstant("MAIL");
        Expression exp3 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        idColumnNameConst = Constants.createColumnNameConstant("l_quantity");
        idValConst = Constants.createLiteralConstant("50");
        Expression exp4 = Expressions.createExpression(Operator.GT, idColumnNameConst, idValConst);
        idColumnNameConst = Constants.createColumnNameConstant("l_extendedprice");
        idValConst = Constants.createLiteralConstant("35000");
        Expression exp5 = Expressions.createExpression(Operator.GT, idColumnNameConst, idValConst);

        // Second compound
        Expression andExp2 = Expressions.createExpression(Operator.AND, exp3, exp4, exp5);

        idColumnNameConst = Constants.createColumnNameConstant("l_partkey");
        idValConst = Constants.createLiteralConstant("90");
        Expression exp6 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);

        // Connecting all compounds
        Expression orExp = Expressions.createExpression(Operator.OR, andExp1, andExp2, exp6);

        QueryStatement q = Statements.buildQueryStatement(selectAll, orExp, LINEITEM);
        // start time
        long startTime = System.nanoTime();
        Relation rel = qLayer.executeQuery(q);
        iterateRelation(rel);
        // elapsed test time
        outputTime("SelectRows-ComplexQuery", scaleFactor, System.nanoTime() - startTime);
    }


    private void testExecuteSelectJoinQuery() throws Exception {
        // Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("l_orderkey");
        Constant idValConst = Constants.createColumnNameConstant("o_orderkey");
        Expression exp1 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);

        idColumnNameConst = Constants.createColumnNameConstant("o_custkey");
        idValConst = Constants.createColumnNameConstant("c_custkey");
        Expression exp2 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);

        idColumnNameConst = Constants.createColumnNameConstant("l_shipmode");
        idValConst = Constants.createLiteralConstant("RAIL");
        Expression exp3 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);

        Expression expAnd = Expressions.createExpression(Operator.AND, exp1, exp2);
        Expression finalExp = Expressions.createExpression(Operator.AND, expAnd, exp3);

        QueryStatement q = Statements.buildQueryStatement(selectAtts, finalExp, LINEITEM, ORDERS, CUSTOMER);
        // start time
        long startTime = System.nanoTime();
        Relation rel = qLayer.executeQuery(q);
        iterateRelation(rel);
        // elapsed test time
        outputTime("SelectRows-JoinQuery", scaleFactor, System.nanoTime() - startTime);
    }
    /*
     * END OF UNIT TESTS
     */

    /*
     * PRIVATE METHODS
     */
    private void createTable(String tableName, List<String> columns, List<Type> types) throws Exception {
        CreateTableStatement st = Statements.buildCreateTableStatement(tableName, columns, types);
        qLayer.createTable(st);
    }

    private void insertRows(String tableName) throws Exception {
        for (int i = 0; i < scaleFactor; i++) {
            InsertRowsStatement insertStmnt = Utils.getInsertStatement(tableName,
                    TPCHData.createColumns(tableName, scaleFactor, i),
                    TPCHData.getBaseSize(tableName));
            qLayer.executeInsertRows(insertStmnt);
        }
    }

    private /* static */ void outputTime(String testCaseName, int scale, long nanoTime) {
        String timeString = NumberFormat.getInstance(Locale.US).format(nanoTime / 1000d / 1000d / 1000d);
        Utils.getOut().println(testCaseName + "\tTime: " + timeString + " seconds");
        this.scaleResults.add("<measurement><name>"+ testCaseName + "</name>"
                               + "<scale>" + scale + "</scale>"
                               + "<value>" + timeString + "</value></measurement>");
    }

    private /* static */ void printMemory(int scale) {
        for (int i = 0; i < 5; ++i) {
            System.gc();
        }
        float footprint = ((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024f / 1024f);
        Utils.getOut().println("Memory footprint: " + footprint + " MB");
        this.scaleResults.add("<measurement><name>footprint</name>"
                               + "<scale>" + scale + "</scale>"
                               + "<value>" + footprint + "</value></measurement>");
    }

    private void iterateRelation(Relation rel) {
        RowCursor rc = rel.getRows();

        while (rc.next()) {
        	checksum += 1;
        }
        
        //Utils.getOut().println("iterateRelation: " + checksum);
    }
    
    private void computeChecksum(RowCursor retrievedRowsCursor) {
    	/*stringC = 0;
    	integerC = 0;
    	doubleC = 0;
    	dateC = 0;
    	booleanC = 0;*/
    	
		if (retrievedRowsCursor.next()) {
			RowMetaData metaData = retrievedRowsCursor.getMetaData();
			types = new Type[metaData.getColumnCount()];
			for (int index = 0; index < metaData.getColumnCount(); ++index) {
				types[index] = metaData.getColumnMetaData(index).getType();
			}
			do {
				getRowsByPrimitives(retrievedRowsCursor);
			} while (retrievedRowsCursor.next());
			
			/*Utils.getOut().println("computeChecksum: " + checksum);
			Utils.getOut().println("\tString: " + stringC);
			Utils.getOut().println("\tInteger: " + integerC);
			Utils.getOut().println("\tDouble: " + doubleC);
			Utils.getOut().println("\tDate: " + dateC);
			Utils.getOut().println("\tBoolean: " + booleanC);*/
		}		
	}
    
	/*long stringC = 0;
	long integerC = 0;
	long doubleC = 0;
	long dateC = 0;
	long booleanC = 0;*/

	private void getRowsByPrimitives(final Row row) {
		for (int i = 0; i < types.length; ++i) {
			if (row.isNull(i)) {
				continue;
			}

			long tmp = 0;
			
			// add up checksum to avoid dead code elimination
			switch (types[i]) {
			case STRING:
				tmp = row.getString(i).hashCode();
				//stringC += tmp;
				break;
			case INTEGER:
				tmp = row.getInteger(i);
				//integerC += tmp;
				break;
			case DOUBLE:
				tmp = Double.doubleToLongBits(row.getDouble(i));
				//doubleC += tmp;
				break;
			case DATE:
				tmp = row.getDate(i).getTime();
				//dateC += tmp;
				break;
			case BOOLEAN:
				tmp = row.getBoolean(i) ? 1 : 0;
				//booleanC += tmp;
				break;
			default:
				throw new RuntimeException();
			}
			
			checksum += tmp;
		}	
	}
    /*
     * END OF PRIVATE METHODS
     */
} // END OF CLASS