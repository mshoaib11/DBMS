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
import dbs_project.query.functional.Statements;
import dbs_project.query.predicate.Constant;
import dbs_project.query.predicate.Expression;
import dbs_project.query.predicate.ExpressionElement;
import dbs_project.query.predicate.Operator;
import dbs_project.query.predicate.impl.Constants;
import dbs_project.query.predicate.impl.Expressions;
import dbs_project.query.statement.DeleteRowsStatement;
import dbs_project.query.statement.InsertRowsStatement;
import dbs_project.util.TPCHData;
import dbs_project.util.Utils;
import org.junit.*;


/**
 *
 */
public class TestShutDown extends TestBase {

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
    public void testShutDown() throws Exception {
        // Create table first
        createTable(c_table, cust_columns, cust_types);

        for (int i = 0; i < SCALE; ++i) {
            // Get rows from input file
            InsertRowsStatement insertStmnt = Utils.getInsertStatement(c_table,
                        TPCHData.createColumns(c_table, SCALE, i),
                        TPCHData.getBaseSize(c_table));

            queryLayer.executeInsertRows(insertStmnt);
        }
        // Delete Query
        Constant idColumnNameConst = Constants.createColumnNameConstant("c_custkey");
        ExpressionElement idValConst = Constants.createLiteralConstant("10");
        Expression exp = Expressions.createExpression(Operator.GEQ, idColumnNameConst, idValConst);
        DeleteRowsStatement dst = Statements.buildDeleteRowsStatement(c_table, exp);

        queryLayer.executeDeleteRows(dst);

        long start = System.nanoTime();
        db.shutDown();
        outputTime("ShutDown", System.nanoTime() - start);
    }

}
