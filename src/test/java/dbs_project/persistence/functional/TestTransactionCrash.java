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
import dbs_project.exceptions.NoTransactionActiveException;
import dbs_project.exceptions.TransactionAlreadyActiveException;
import dbs_project.query.functional.Statements;
import dbs_project.query.predicate.Constant;
import dbs_project.query.predicate.Expression;
import dbs_project.query.predicate.Operator;
import dbs_project.query.predicate.impl.Constants;
import dbs_project.query.predicate.impl.Expressions;
import dbs_project.query.statement.DeleteRowsStatement;
import dbs_project.query.statement.InsertRowsStatement;
import dbs_project.query.statement.UpdateRowsStatement;
import dbs_project.util.TPCHData;
import dbs_project.util.Utils;
import java.util.Arrays;
import java.util.List;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 */
public class TestTransactionCrash extends TestBase {
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
    }

    /**
     * Test of beginTransaction method, of class PersistenceLayer.
     */
    @Test(timeout = 300000L)
    public void testTransaction() throws Exception {
        long time = 0, start = System.nanoTime();
        // Create table first
        createTable(c_table, cust_columns, cust_types);

        // Committed Transaction Block (two queries)
        assertFalse(persistenceLayer.hasActiveTransaction());
        try {
            persistenceLayer.commitTransaction();
            fail("You shouldn't be able to commit");
        } catch (NoTransactionActiveException e) {
        }
        persistenceLayer.beginTransaction();
        assertTrue(persistenceLayer.hasActiveTransaction());
        try {
            persistenceLayer.beginTransaction();
            fail("You should have an active transaction");
        } catch (TransactionAlreadyActiveException e) {
        }
        time += System.nanoTime() - start;

        for (int i = 0; i < SCALE; ++i) {
            // Get rows from input file
            InsertRowsStatement insertStmnt = Utils.getInsertStatement(c_table,
                        TPCHData.createColumns(c_table, SCALE, i),
                        TPCHData.getBaseSize(c_table));
            start = System.nanoTime();
            queryLayer.executeInsertRows(insertStmnt);
            time += System.nanoTime() - start;
        }
        Constant idColumnNameConst = Constants.createColumnNameConstant("c_nationkey");
        Constant idValConst = Constants.createLiteralConstant("2");
        Expression exp = Expressions.createExpression(Operator.EQ, idColumnNameConst, idValConst);
        UpdateRowsStatement ust = Statements.buildUpdateRowStatement(c_table, cust_columns_update, exp, cust_values_update);

        start = System.nanoTime();
        queryLayer.executeUpdateRows(ust);
        persistenceLayer.commitTransaction();
        time += System.nanoTime() - start;
        assertFalse(persistenceLayer.hasActiveTransaction());

        // Uncommitted Transaction
        idColumnNameConst = Constants.createColumnNameConstant("c_custkey");
        idValConst = Constants.createLiteralConstant("10");
        exp = Expressions.createExpression(Operator.LEQ, idColumnNameConst, idValConst);
        DeleteRowsStatement dst = Statements.buildDeleteRowsStatement(c_table, exp);

        start = System.nanoTime();
        persistenceLayer.beginTransaction();
        queryLayer.executeDeleteRows(dst);
        time += System.nanoTime() - start;
        outputTime("Transaction-Insert&Update", time);
    }

}
