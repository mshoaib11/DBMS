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

package dbs_project.database;

import dbs_project.index.IndexLayer;
import dbs_project.persistence.PersistenceLayer;
import dbs_project.query.QueryLayer;
import dbs_project.storage.StorageLayer;

import java.io.IOException;

/**
 * This class provides access to your database implementation.
 * <p/>
 * We will activate the remaining layers for every milestone.
 */
public interface Database {

    /**
     * Implement for milestone 1!
     *
     * @return
     */
    StorageLayer getStorageLayer();

    /**
     * Implement for milestone 2!
     *
     * @return
     */
    IndexLayer getIndexLayer();

    /**
     * Implement for milestone 3!
     *
     * @return
     */
    QueryLayer getQueryLayer();

    /**
     * Implement for final hand-in!
     *
     * @return
     */
    PersistenceLayer getPersistenceLayer();

    /**
     * Persist the state of the database to stable storage.
     *
     * Implement for final hand-in!
     * 
     * @throws java.io.IOException
     */
    void shutDown() throws IOException;

    /**
     * Start the Database and restore the state (tables, indexes,...) that was
     * written to stable storage by the last shutDown().
     * <p/>
     * Also perform crash recovery in case the database was not shut down
     * properly. Redo all committed transactions and (if needed) undo the
     * incomplete transaction.
     *
     * Implement for final hand-in!
     * 
     * @throws IOException
     */
    void startUp() throws IOException;
    
    /**
     * Delete all files created by our database from disk.
     * 
     * Implement for final hand-in!
     * 
     * @throws IOException
     *      
     */
    void deleteDatabaseFiles() throws IOException;
}
