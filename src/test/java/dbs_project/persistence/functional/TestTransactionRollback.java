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

import dbs_project.database.DatabaseFactory;
import dbs_project.query.functional.DerbyDB;
import dbs_project.query.functional.Statements;
import dbs_project.query.predicate.Constant;
import dbs_project.query.predicate.Expression;
import dbs_project.query.predicate.Operator;
import dbs_project.query.predicate.impl.Constants;
import dbs_project.query.predicate.impl.Expressions;
import dbs_project.query.statement.DeleteRowsStatement;
import dbs_project.query.statement.InsertRowsStatement;
import dbs_project.query.statement.UpdateRowsStatement;
import dbs_project.util.SimpleColumn;
import dbs_project.util.TPCHData;
import dbs_project.util.Utils;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 */
public class TestTransactionRollback extends TestBase {
    private static final List<String> cust_columns_update = Arrays.asList("c_comment");
    private static final List<String> cust_values_update = Arrays.asList("the next football worldcup is gonna be there");

    @Before
    public void setUp() throws Exception {
        Utils.RANDOM.setSeed(SEED);
        this.db = DatabaseFactory.INSTANCE.createInstance();
        this.db.getPersistenceLayer().setPersistence(true);
        this.db.startUp();
        
        this.persistenceLayer = db.getPersistenceLayer();
        this.queryLayer = db.getQueryLayer();
        
        this.derby = new DerbyDB();
    }

    @After
    public void tearDown() throws Exception {
        db.deleteDatabaseFiles();
        derby.close();
    }

    /**
     * Test of beginTransaction method, of class PersistenceLayer.
     */
    @Test(timeout = 300000L)
    public void testRollback() throws Exception {
        // Create table first
        createTable(c_table, cust_columns, cust_types);
        createDerbyTable(c_table, cust_columns, cust_types);

        persistenceLayer.beginTransaction();
        derby.beginTransaction();
        assertTrue(persistenceLayer.hasActiveTransaction());

        for (int i = 0; i < SCALE; ++i) {
            // Get rows from input file
            List<SimpleColumn> columns = TPCHData.createColumns(c_table, SCALE, i);
            int rowCount = TPCHData.getBaseSize(c_table);
            InsertRowsStatement insertStmnt =
                    Utils.getInsertStatement(c_table, columns, rowCount);

            queryLayer.executeInsertRows(insertStmnt);
            derby.insertData(c_table, columns, rowCount);
        }
        Constant idColumnNameConst = Constants.createColumnNameConstant("c_nationkey");
        Constant idValConst = Constants.createLiteralConstant("2");
        Expression exp = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        UpdateRowsStatement ust = Statements.buildUpdateRowStatement(c_table, cust_columns_update, exp, cust_values_update);

        queryLayer.executeUpdateRows(ust);
        persistenceLayer.commitTransaction();

        derby.update(c_table, "c_comment='the next football worldcup is gonna be there'", "c_nationkey=2");
        derby.commit();

        // Uncommitted Transaction
        idColumnNameConst = Constants.createColumnNameConstant("c_custkey");
        idValConst = Constants.createLiteralConstant("10");
        exp = Expressions.createExpression(Operator.LEQ, idColumnNameConst, idValConst);
        DeleteRowsStatement dst = Statements.buildDeleteRowsStatement(c_table, exp);

        persistenceLayer.beginTransaction();
        queryLayer.executeDeleteRows(dst);
        derby.beginTransaction();
        derby.delete(c_table, "c_custkey <= 10");
        tableCompare(c_table);

        long start = System.nanoTime();
        persistenceLayer.abortTransaction();
        outputTime("Rollback", System.nanoTime() - start);
        derby.abort();
        tableCompare(c_table);
    }

}
