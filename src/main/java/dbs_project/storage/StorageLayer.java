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

package dbs_project.storage;

import dbs_project.exceptions.NoSuchTableException;
import dbs_project.exceptions.TableAlreadyExistsException;
import dbs_project.util.annotation.NotNull;

import java.util.Collection;
import java.util.Map;

/**
 * The storage layer of the database.
 */
public interface StorageLayer {

    /**
     * Create a new table
     *
     * @param tableName Name of the table to be created
     * @param schema    Schema as map column name -> type
     * @return Unique id of the new table
     * @throws TableAlreadyExistsException Table with same name already in DB
     */
    int createTable(@NotNull String tableName, @NotNull Map<String, Type> schema)
            throws TableAlreadyExistsException;

    /**
     * Delete a table
     *
     * @param tableId Unique id of the table to be deleted
     * @throws NoSuchTableException Table with given name does not exist
     */
    void deleteTable(int tableId) throws NoSuchTableException;

    /**
     * @param tableId Unique id of the table to be renamed
     * @param newName New table name
     * @throws TableAlreadyExistsException Table already exists
     * @throws NoSuchTableException        No such table
     */
    void renameTable(int tableId, @NotNull String newName)
            throws TableAlreadyExistsException, NoSuchTableException;

    /**
     * @param tableId Unique id of the table
     * @return A reference to the table
     * @throws NoSuchTableException Table with given name does not exist
     */
    @NotNull
    Table getTable(int tableId) throws NoSuchTableException;

    /**
     * @return A collection providing references to all tables in the database
     */
    @NotNull
    Collection<Table> getTables();

    /**
     * @return Mapping table name -> table meta data for all tables in the db
     *         (use a copy of the data here)
     */
    @NotNull
    Map<String, TableMetaData> getDatabaseSchema();

}
