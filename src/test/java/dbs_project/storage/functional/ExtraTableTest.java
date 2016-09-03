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

package dbs_project.storage.functional;

import dbs_project.database.DatabaseFactory;
import dbs_project.exceptions.ColumnAlreadyExistsException;
import dbs_project.exceptions.NoSuchColumnException;
import dbs_project.exceptions.NoSuchRowException;
import dbs_project.storage.*;
import dbs_project.util.*;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.log4j.Logger;
import org.junit.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Functional tests for Table
 */
public final class ExtraTableTest {

    private StorageLayer storage;
   

    public ExtraTableTest() {

    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    	Utils.redirectStreams();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    	Utils.revertStreams();
    }

    @Before
    public void setUp() {
        storage = DatabaseFactory.INSTANCE.createInstance().getStorageLayer();
    }

    @After
    public void tearDown() throws Exception {
    	
    }

    @Test(timeout = 300000L)
    public void testGetRowsException() throws Exception {
        Utils.getOut().println("testGetRowsException");
        
        final Map<String, Type> tableSchema = new HashMap<String, Type>();
        tableSchema.put("aColumn", Type.STRING);
        
        int tableID = storage.createTable(StorageLayerTest.TABLE_NAME_1, tableSchema);
        Table table = storage.getTable(tableID);
        
        RowCursor rc = new RandomRowCursor(tableSchema, 1);
        IdCursor ids = table.addRows(rc.getMetaData(), rc);
        assertTrue("idCursor should more elements", ids.next()); //we added one row (index 0) so this test case must pass
        
        int rowId = ids.getId();
        assertFalse("idCursor should not have more elemenrs", ids.next()); //index 1 is not present as we only added one row
        
        try {
        	table.getRows(OneIntIteratorWrapper.wrap(rowId + 1));
        	fail("NoSuchRowException expected"); //caught in Row getRow(int rowId)
        } catch(NoSuchRowException e) {
        	
        }
    }
  
}
