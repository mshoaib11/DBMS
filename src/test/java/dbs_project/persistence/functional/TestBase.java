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

package dbs_project.persistence.functional;

import dbs_project.database.Database;
import dbs_project.exceptions.QueryExecutionException;
import dbs_project.persistence.PersistenceLayer;
import dbs_project.query.QueryLayer;
import dbs_project.query.functional.DerbyDB;
import dbs_project.query.functional.Statements;
import dbs_project.query.statement.CreateTableStatement;
import dbs_project.query.statement.QueryStatement;
import dbs_project.storage.RowCursor;
import dbs_project.storage.Type;
import dbs_project.util.ResultComparator;
import dbs_project.util.Utils;

import java.sql.ResultSet;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class TestBase {

    protected static final int SCALE = 100;
    protected static final long SEED = 1337L;
    protected static final String c_table = "CUSTOMER";
    protected static final List<String> cust_columns = Arrays.asList("c_custkey", "c_name", "c_address", "c_nationkey",
            "c_phone", "c_acctbal", "c_mktsegment", "c_comment");
    protected static final List<Type> cust_types = Arrays.asList(Type.INTEGER, Type.STRING, Type.STRING,
            Type.INTEGER, Type.STRING, Type.DOUBLE, Type.STRING, Type.STRING);
    protected PersistenceLayer persistenceLayer;
    protected QueryLayer queryLayer;
    protected Database db;
    protected DerbyDB derby;
    protected static List<String> results;

    protected void createTable(String tableName, List<String> columns, List<Type> types) throws QueryExecutionException {
        CreateTableStatement st = Statements.buildCreateTableStatement(tableName, columns, types);

        queryLayer.createTable(st);
    }

    protected void createDerbyTable(String tableName, List<String> columns, List<Type> types) throws QueryExecutionException {
        CreateTableStatement st = Statements.buildCreateTableStatement(tableName, columns, types);

        derby.executeSQL(Statements.toSQL(st));
    }

    protected void tableCompare(String tableName) throws Exception {
        // SQL query to validate
        String sql = "SELECT * FROM \"" + tableName + "\"";
        QueryStatement localSQL = Statements.buildQueryStatement(
                Arrays.asList(tableName), Arrays.asList("*"), null);

        RowCursor rc = queryLayer.executeQuery(localSQL).getRows();
        ResultSet rs = derby.executeQuery(sql);
        assertTrue("Set of returned rows is different than the set returned by derbyderby", ResultComparator.compareResultsSimple(rs, rc));
    }

    protected static void outputTime(String testCaseName, long nanoTime) {
        if (results == null) results = new ArrayList<>();
        String timeString = NumberFormat.getInstance(Locale.US).format(nanoTime / 1000d / 1000d / 1000d);
        Utils.getOut().println(testCaseName + "\tTime: " + timeString + " seconds");
       results.add("<measurement><name>"+ testCaseName.replace("&", "&amp;") + "</name>"
                               + "<scale>" + SCALE + "</scale>"
                               + "<value>" + timeString + "</value></measurement>");
    }

}
