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

import dbs_project.util.TPCHData;
import dbs_project.util.Utils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({TestTransactionCrash.class, TestTransactionValidate.class,
                     TestAutoCommitCrash.class, TestAutoCommitValidate.class,
                     TestShutDown.class, TestStartUp.class,
                     TestTransactionRollback.class})
public class PersistenceLayerTestSuite {

    @BeforeClass
    public static void setUpClass() throws Exception {
    	Utils.redirectStreams();
    	
        TPCHData.CUSTOMER_BASE_SIZE = 3000;
        Utils.getOut().println();
        Utils.getOut().println("Starting persistennce layer tests.");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        Utils.getOut().println();
        Utils.getOut().println("<measurements layer=\"persistence\">");
        if(TestBase.results != null) {
	        for (String res : TestBase.results) {
	            Utils.getOut().println(res);
	        }
        }
        Utils.getOut().println("</measurements>");
        Utils.getOut().println();
        
        Utils.revertStreams();
    }

}
