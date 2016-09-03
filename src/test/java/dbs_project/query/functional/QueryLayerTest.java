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

package dbs_project.query.functional;

import dbs_project.database.DatabaseFactory;
import dbs_project.exceptions.QueryExecutionException;
import dbs_project.index.IndexType;
import dbs_project.query.QueryLayer;
import dbs_project.query.predicate.Constant;
import dbs_project.query.predicate.Expression;
import dbs_project.query.predicate.Operator;
import dbs_project.query.predicate.impl.Constants;
import dbs_project.query.predicate.impl.Expressions;
import dbs_project.query.statement.*;
import dbs_project.storage.RowCursor;
import dbs_project.storage.Type;
import dbs_project.util.ResultComparator;
import dbs_project.util.SimpleColumn;
import dbs_project.util.TPCHData;
import dbs_project.util.TestTableBuilder;
import dbs_project.util.Utils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.junit.*;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class QueryLayerTest {

    private DerbyDB db;
    private QueryLayer qLayer;
    private static final String CUSTOMER = "CUSTOMER";
    private static final String NATION = "nation";
    private static final String REGION = "region";
    private static final List<String> cust_columns = Arrays.asList(
            "c_custkey", "c_name", "c_address", "c_nationkey",
            "c_phone", "c_acctbal", "c_mktsegment", "c_comment");
    private static final List<Type> cust_types = Arrays.asList(
            Type.INTEGER, Type.STRING, Type.STRING, Type.INTEGER,
            Type.STRING, Type.DOUBLE, Type.STRING, Type.STRING);
    private static final List<String> nation_columns =
            Arrays.asList("n_nationkey", "n_name", "n_regionkey", "n_comment");
    private static final List<Type> nation_types =
            Arrays.asList(Type.INTEGER, Type.STRING, Type.INTEGER, Type.STRING);
    private static final List<String> region_columns =
            Arrays.asList("r_regionkey", "r_name", "r_comment");
    private static final List<Type> region_types =
            Arrays.asList(Type.INTEGER, Type.STRING, Type.STRING);
    private static final QueryStatement dummySQL = Statements.buildQueryStatement(
            Arrays.asList(CUSTOMER), Arrays.asList("*"), null);
    private static final List<String> simpleSelectPointQueryProjection = Arrays.asList("c_name", "c_address");
    private static final List<String> simpleSelectRangeQueryProjection = Arrays.asList("c_name");
    private static final List<String> simpleSelectJoinQueryProjection = Arrays.asList("c_name", "r_name");
    private static final List<String> cust_columns_update = Arrays.asList("c_comment");
    private static final List<String> cust_values_update =
            Arrays.asList("the next football worldcup is gonna be there");

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
        qLayer = DatabaseFactory.INSTANCE.createInstance().getQueryLayer();
        db = new DerbyDB();
    }

    @After
    public void tearDown() throws Exception {
        db.close();
    }

    /*
     * UNIT TESTS
     */
    @Test(timeout = 300000L)
    public void testCreateTable() throws Exception {
        CreateTableStatement st = Statements.buildCreateTableStatement(CUSTOMER, cust_columns, cust_types);

        qLayer.createTable(st);

        try {
            qLayer.createTable(st);
            fail("Should have received an exception when creating the customer table twice");
        } catch (QueryExecutionException e) {
            // Everything is ok!
        }
        db.executeSQL(Statements.toSQL(st));
        tableCompare(CUSTOMER);
    }

    @Test(timeout = 300000L)
    public void testRenameTable() throws Exception {
        // Create table first
        createTable("test1", cust_columns, cust_types);
        createTable("test2", cust_columns, cust_types);
        // Rename
        RenameTableStatement st1 = Statements.buildRenameTableStatement("test1", CUSTOMER);
        RenameTableStatement st2 = Statements.buildRenameTableStatement("test2", CUSTOMER);
        RenameTableStatement st3 = Statements.buildRenameTableStatement("test2", "test1");

        qLayer.renameTable(st1);
        db.executeSQL(Statements.toSQL(st1));

        try {
            qLayer.renameTable(st1);
            fail("Should have received an exception when renaming the test table twice");
        } catch (QueryExecutionException e) {
            // Everything is ok!
        }
        try {
            qLayer.renameTable(st2);
            fail("Should have received an exception when renaming would cause name duplication");
        } catch (QueryExecutionException e) {
            // Everything is ok!
        }
        qLayer.renameTable(st3);
        tableCompare(CUSTOMER);
    }

    @Test(timeout = 300000L)
    public void testCreateColumn() throws Exception {
        // Create table first
        createTable(CUSTOMER, cust_columns, cust_types);
        // Create column
        CreateColumnStatement st = Statements.buildCreateColumnStatement(CUSTOMER, "dummy", Type.INTEGER);

        qLayer.createColumn(st);

        try {
            qLayer.createColumn(st);
            fail("Should have received an exception when creating an existing column in the customer table");
        } catch (QueryExecutionException e) {
            // Everything is ok!
        }
        db.executeSQL(Statements.toSQL(st));
        tableCompare(CUSTOMER);
    }

    @Test(timeout = 300000L)
    public void testDropTable() throws QueryExecutionException {
        // Create table first
        createTable(CUSTOMER, cust_columns, cust_types);
        // Drop the table
        DropTableStatement   dtst = Statements.buildDropTableStatement(CUSTOMER);
        CreateTableStatement ctst = Statements.buildCreateTableStatement(CUSTOMER, cust_columns, cust_types);

        qLayer.dropTable(dtst);

        try {
            qLayer.dropTable(dtst);
            fail("Should have received an exception when droping the customer table twice");
        } catch (QueryExecutionException e) {
            // Everything is ok!
        }
        try {
            qLayer.executeQuery(dummySQL);
            fail("Should have received an exception when querying the dropped customer table");
        } catch (QueryExecutionException e) {
            // Everything is ok!
        }
        qLayer.createTable(ctst);
    }

    @Test(timeout = 300000L)
    public void testDropColumn() throws Exception {
        // Create table first
        createTable(CUSTOMER, cust_columns, cust_types);
        // Drop a column
        DropColumnStatement   dcst = Statements.buildDropColumnStatement(CUSTOMER, "c_phone");
        CreateColumnStatement ccst = Statements.buildCreateColumnStatement(CUSTOMER, "c_phone", Type.DOUBLE);

        qLayer.dropColumn(dcst);

        try {
            qLayer.dropColumn(dcst);
            fail("Should have received an exception when dropping a dropped column in the customer table");
        } catch (QueryExecutionException e) {
            // Everything is ok!
        }
        db.executeSQL(Statements.toSQL(dcst));
        tableCompare(CUSTOMER);
        qLayer.createColumn(ccst);
    }

    @Test(timeout = 300000L)
    public void testRenameColumn() throws Exception {
        // Create table first
        createTable(CUSTOMER, cust_columns, cust_types);
        // Rename
        RenameColumnStatement st1 = Statements.buildRenameColumnStatement(CUSTOMER, "c_name", "c_firstname");
        RenameColumnStatement st2 = Statements.buildRenameColumnStatement(CUSTOMER, "c_mktsegment", "c_firstname");
        RenameColumnStatement st3 = Statements.buildRenameColumnStatement(CUSTOMER, "c_mktsegment", "c_name");

        qLayer.renameColumn(st1);
        db.executeSQL(Statements.toSQL(st1));
        tableCompare(CUSTOMER);

        try {
            qLayer.renameColumn(st1);
            fail("Should have received an exception when renaming a non-exiting column in the customer table");
        } catch (QueryExecutionException e) {
            // Everything is ok!
        }
        try {
            qLayer.renameColumn(st2);
            fail("Should have received an exception when renaming would cause name duplication");
        } catch (QueryExecutionException e) {
            // Everything is ok!
        }

        qLayer.renameColumn(st3);
        db.executeSQL(Statements.toSQL(st3));
        tableCompare(CUSTOMER);
    }

    @Test(timeout = 300000L)
    public void testCreateAndDropIndex() throws Exception {
        // Create table first
        createTable(CUSTOMER, cust_columns, cust_types);
        // Create column
        CreateIndexStatement cist = Statements.buildCreateIndexStatement(
                CUSTOMER, "c_custkey", "c_custkey_tree", IndexType.TREE);
        DropIndexStatement   dist = Statements.buildDropIndexStatement(
                CUSTOMER, "c_custkey", "c_custkey_tree");

        qLayer.createIndex(cist);

        try {
            qLayer.createIndex(cist);
            fail("Should have received an exception when creating an existing index in the customer table");
        } catch (QueryExecutionException e) {
            // Everything is ok!
        }
        qLayer.dropIndex(dist);

        try {
            qLayer.dropIndex(dist);
            fail("Should have received an exception when dropping a dropped index in the customer table");
        } catch (QueryExecutionException e) {
            // Everything is ok!
        }
        qLayer.createIndex(cist);

        tableCompare(CUSTOMER);
    }

    @Test(timeout = 300000L)
    public void testInsertRows() throws Exception {
        // Create table first
        createTable(CUSTOMER, cust_columns, cust_types);

        // Get rows from input file
        List<SimpleColumn> columns = TPCHData.createCustomerColumns(1, 0);
        InsertRowsStatement insertStmnt = Utils.getInsertStatement(
                CUSTOMER, columns, TPCHData.CUSTOMER_BASE_SIZE);

        qLayer.executeInsertRows(insertStmnt);

        // Import data into Derby
        db.insertData(CUSTOMER, columns, TPCHData.CUSTOMER_BASE_SIZE);
        tableCompare(CUSTOMER);
    }

    @Test(timeout = 300000L)
    public void testDeleteRows() throws Exception {
        // Create table first
        createTable(CUSTOMER, cust_columns, cust_types);
        // Insert rows
        insertCustomerRows();

        // Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("c_custkey");
        Constant idValConst = Constants.createLiteralConstant("10");
        Expression exp = Expressions.createExpression(Operator.LEQ, idColumnNameConst, idValConst);
        DeleteRowsStatement st = Statements.buildDeleteRowsStatement(CUSTOMER, exp);

        int nbRows = qLayer.executeDeleteRows(st);
        int nbRowsDerby = db.delete(CUSTOMER, "c_custkey<=10");
        assertEquals("Number of deleted rows is incorrect", nbRowsDerby, nbRows);
        tableCompare(CUSTOMER);
    }

    @Test(timeout = 300000L)
    public void testUpdateRows() throws Exception {
        // Create table first
        createTable(CUSTOMER, cust_columns, cust_types);
        // Insert rows
        insertCustomerRows();

        // Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("c_nationkey");
        Constant idValConst = Constants.createLiteralConstant("2");
        Expression exp = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        UpdateRowsStatement st = Statements.buildUpdateRowStatement(
                CUSTOMER, cust_columns_update, exp, cust_values_update);

        int nbRows = qLayer.executeUpdateRows(st);
        int nbRowsDerby = db.update(CUSTOMER, "c_comment='" + cust_values_update.get(0) + "'", "c_nationkey=2");
        assertEquals("Number of updated rows is incorrect", nbRowsDerby, nbRows);
        tableCompare(CUSTOMER);
    }

    @Test(timeout = 300000L)
    public void testExecuteSelectPointQuery() throws Exception {
        // SQL query to run
        String sql = "SELECT c_name, c_address FROM " + CUSTOMER + " WHERE c_custkey = 42";

        // Create table first
        createTable(CUSTOMER, cust_columns, cust_types);
        // Insert rows
        insertCustomerRows();

        // Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("c_custkey");
        Constant idValConst = Constants.createLiteralConstant("42");
        Expression exp = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        QueryStatement st = Statements.buildQueryStatement(simpleSelectPointQueryProjection, exp, CUSTOMER);

        RowCursor rc = qLayer.executeQuery(st).getRows();
        ResultSet rs = db.executeQuery(sql);
        assertTrue("Set of returned rows is different than the set returned by derbyDB",
                ResultComparator.compareResultsSimple(rs, rc));
    }

    @Test(timeout = 300000L)
    public void testExecuteSelectRangeQuery() throws Exception {
        // SQL query to run
        String sql = "SELECT c_name FROM " + CUSTOMER
                + " WHERE c_acctbal > 5000 AND c_mktsegment = 'AUTOMOBILE'";

        // Create table first
        createTable(CUSTOMER, cust_columns, cust_types);
        // Insert rows
        insertCustomerRows();

        // Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("c_acctbal");
        Constant idValConst = Constants.createLiteralConstant("5000");
        Expression exp1 = Expressions.createExpression(Operator.GT, idColumnNameConst, idValConst);
        idColumnNameConst = Constants.createColumnNameConstant("c_mktsegment");
        idValConst = Constants.createLiteralConstant("AUTOMOBILE");
        Expression exp2 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        Expression exp = Expressions.createExpression(Operator.AND, exp1, exp2);
        QueryStatement st = Statements.buildQueryStatement(simpleSelectRangeQueryProjection, exp, CUSTOMER);

        RowCursor rc = qLayer.executeQuery(st).getRows();
        ResultSet rs = db.executeQuery(sql);
        assertTrue("Set of returned rows is different than the set returned by derbyDB",
                ResultComparator.compareResultsSimple(rs, rc));
    }

    @Test(timeout = 300000L)
    public void testExecuteComplpexSelectQuery() throws Exception {
        // SQL query to run
        String sql = "SELECT c_name, c_address FROM " + CUSTOMER
                + " WHERE (c_mktsegment = 'BUILDING' AND c_acctbal > 3000)"
                + " OR (c_mktsegment = 'HOUSEHOLD' AND c_acctbal < 3000 AND c_nationkey = 23)"
                + " OR c_nationkey = 6";

        // Create table first
        createTable(CUSTOMER, cust_columns, cust_types);
        // Insert rows
        insertCustomerRows();

        // Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("c_mktsegment");
        Constant idValConst = Constants.createLiteralConstant("BUILDING");
        Expression exp1 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        idColumnNameConst = Constants.createColumnNameConstant("c_acctbal");
        idValConst = Constants.createLiteralConstant("3000");
        Expression exp2 = Expressions.createExpression(Operator.GT, idColumnNameConst, idValConst);

        // First compound
        Expression andExp1 = Expressions.createExpression(Operator.AND, exp1, exp2);

        idColumnNameConst = Constants.createColumnNameConstant("c_mktsegment");
        idValConst = Constants.createLiteralConstant("HOUSEHOLD");
        Expression exp3 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        idColumnNameConst = Constants.createColumnNameConstant("c_acctbal");
        idValConst = Constants.createLiteralConstant("3000");
        Expression exp4 = Expressions.createExpression(Operator.LT, idColumnNameConst, idValConst);
        idColumnNameConst = Constants.createColumnNameConstant("c_nationkey");
        idValConst = Constants.createLiteralConstant("23");
        Expression exp5 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);

        // Second compound
        Expression andExp2 = Expressions.createExpression(Operator.AND, exp3, exp4, exp5);

        idColumnNameConst = Constants.createColumnNameConstant("c_nationkey");
        idValConst = Constants.createLiteralConstant("6");
        Expression exp6 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);

        // Connecting compounds
        Expression orExp = Expressions.createExpression(Operator.OR, andExp1, andExp2, exp6);

        QueryStatement st = Statements.buildQueryStatement(simpleSelectPointQueryProjection, orExp, CUSTOMER);

        RowCursor rc = qLayer.executeQuery(st).getRows();
        ResultSet rs = db.executeQuery(sql);
        assertTrue("Set of returned rows is different than the set returned by derbyDB",
                ResultComparator.compareResultsSimple(rs, rc));

    }

    @Test(timeout = 300000L)
    public void testExecuteSelectJoinQuery() throws Exception {
        // SQL query to run
        String sql = "SELECT c_name, r_name FROM " + CUSTOMER + ", \"nation\", \"region\""
                + " WHERE c_nationkey = n_nationkey"
                + " AND n_regionkey = r_regionkey AND c_mktsegment = 'BUILDING'";

        // Create tables first
        createTable(CUSTOMER, cust_columns, cust_types);
        createTable(NATION, nation_columns, nation_types);
        createTable(REGION, region_columns, region_types);
        // Insert rows
        insertCustomerRows();
        insertRows(NATION);
        insertRows(REGION);

        // Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("c_nationkey");
        Constant idValConst = Constants.createColumnNameConstant("n_nationkey");
        Expression exp1 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);

        idColumnNameConst = Constants.createColumnNameConstant("n_regionkey");
        idValConst = Constants.createColumnNameConstant("r_regionkey");
        Expression exp2 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);

        idColumnNameConst = Constants.createColumnNameConstant("c_mktsegment");
        idValConst = Constants.createLiteralConstant("BUILDING");
        Expression exp3 = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);

        Expression expAnd = Expressions.createExpression(Operator.AND, exp1, exp2);
        Expression finalExp = Expressions.createExpression(Operator.AND, expAnd, exp3);

        QueryStatement st = Statements.buildQueryStatement(
                simpleSelectJoinQueryProjection, finalExp, CUSTOMER, NATION, REGION);

        RowCursor rc = qLayer.executeQuery(st).getRows();
        ResultSet rs = db.executeQuery(sql);
        assertTrue("Set of returned rows is different than the set returned by derbyDB",
                ResultComparator.compareResultsSimple(rs, rc));
    }
    /*
     * END OF UNIT TESTS
     */

    private void createTable(String tableName, List<String> columns, List<Type> types)
            throws QueryExecutionException {
        CreateTableStatement st = Statements.buildCreateTableStatement(tableName, columns, types);

        qLayer.createTable(st);
        db.executeSQL(Statements.toSQL(st));
    }

    private void insertRows(String tableName) throws Exception {
        // Get rows from input file
        InsertRowsStatement insertStmnt = Utils.getInsertStatementFromFile(
                TestTableBuilder.class.getResourceAsStream(
                "/data/" + tableName + ".tbl"), tableName);

        qLayer.executeInsertRows(insertStmnt);

        // Import data into Derby
        db.importData(tableName, new File(TestTableBuilder.class.getResource(
                "/data/" + tableName + "Derby.tbl").toURI()).getPath());
    }

    private void insertCustomerRows() throws Exception {
        // Generate data
        List<SimpleColumn> columns = TPCHData.createCustomerColumns(1, 0);
        InsertRowsStatement insertStmnt = Utils.getInsertStatement(
                CUSTOMER, columns, TPCHData.CUSTOMER_BASE_SIZE);

        qLayer.executeInsertRows(insertStmnt);
        db.insertData(CUSTOMER, columns, TPCHData.CUSTOMER_BASE_SIZE);
    }

    private void tableCompare(String tableName) throws Exception {
        // SQL query to validate
        String sql = "SELECT * FROM \"" + tableName + "\"";
        QueryStatement localSQL = Statements.buildQueryStatement(
                Arrays.asList(tableName), Arrays.asList("*"), null);

        RowCursor rc = qLayer.executeQuery(localSQL).getRows();
        ResultSet rs = db.executeQuery(sql);
        assertTrue("Set of returned rows is different than the set returned by derbyDB",
                ResultComparator.compareResultsSimple(rs, rc));
    }
}
