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

package dbs_project.index;

import dbs_project.exceptions.NoSuchTableException;
import dbs_project.storage.StorageLayer;
import dbs_project.util.annotation.NotNull;

import java.util.Collection;

/**
 * The IndexLayer interface represents the indexing layer of the database. Logically, it sits on
 * top of the storage layer to provide additional functionality for more efficient access.
 * <p/>
 * The IndexLayer is designed as a decorator to the StorageLayer and contains
 * the same methods except that it delivers IndexableTables.
 * <p/>
 * However, as this interface extends the StorageLayer interface, you could
 * extend your StorageLayer implementation by replacing the "extends
 * StorageLayer" with "extends IndexLayer"
 */
public interface IndexLayer extends StorageLayer {

    /**
     * @param tableId Unique id of the table
     * @return A reference to the table
     * @throws NoSuchTableException Table with given name does not exist
     */
    @Override
    @NotNull
    IndexableTable getTable(int tableId) throws NoSuchTableException;

    /**
     * @return A collection providing references to all tables in the database
     *         (use a copy of the data here)
     */    
    @NotNull
    Collection<IndexableTable> getIndexableTables();
}
