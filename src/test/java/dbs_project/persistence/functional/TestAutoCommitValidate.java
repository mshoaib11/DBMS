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
import dbs_project.util.TPCHData;
import dbs_project.util.Utils;
import org.junit.*;

/**
 *
 */
public class TestAutoCommitValidate extends TestBase {

    @Before
    public void setUp() throws Exception {
        Utils.RANDOM.setSeed(SEED);
        this.db = DatabaseFactory.INSTANCE.createInstance();
        this.db.getPersistenceLayer().setPersistence(true);
        
        this.persistenceLayer = db.getPersistenceLayer();
        this.queryLayer = db.getQueryLayer();
        
        this.derby = new DerbyDB();
        createDerbyTable(c_table, cust_columns, cust_types);
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
    public void testAutoCommit() throws Exception {
        // Import data into Derby
        for (int i = 0; i < SCALE; ++i) {
            derby.insertData(c_table, TPCHData.createColumns(c_table, SCALE, i),
                                      TPCHData.getBaseSize(c_table));
        }
        derby.update(c_table, "c_comment='the next football worldcup is gonna be there'", "c_nationkey=2");

        long start = System.nanoTime();
        db.startUp();
        outputTime("AutoCommit-Recovery", System.nanoTime() - start);

        tableCompare(c_table);
    }

}
