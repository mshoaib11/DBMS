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

import dbs_project.exceptions.IndexAlreadyExistsException;
import dbs_project.exceptions.NoSuchColumnException;
import dbs_project.exceptions.NoSuchIndexException;
import dbs_project.storage.Table;
import dbs_project.util.annotation.NotNull;

import java.util.Collection;

/**
 * This interface represents an table that can create indexes on its columns.
 * <p/>
 * This interface is designed as a decorator to Table and contains
 * the same methods and some additional functions for indexes.
 * <p/>
 * IndexableTableImpl could be implemented as a wrapper for TableImpl, that
 * keeps track of inserts, deletes, etc..
 * <p/>
 * However, as this interface extends the Table interface, you could also
 * extend your existing Table implementation by replacing the "extends Table"
 * with "extends IndexableTable".
 */
public interface IndexableTable extends Table {

    /**
     * Create a new index of the given type for one column of the table. Once
     * created, the index should reflect the current data in the column as well
     * as any changes to it. It is automatically dropped if the key column is
     * dropped.
     *
     * @param indexName        A name that is unique within this table.
     * @param keyColumnId  The id of the attribute that will serve as the key for this
     *                         index.
     * @param indexType        The type of index to create.
     * @return index id that is unique within this table
     * @throws IndexAlreadyExistsException An index with the supplied name already exists.
     */
    int createIndex(@NotNull String indexName,
                    int keyColumnId, IndexType indexType)
            throws IndexAlreadyExistsException,
            NoSuchColumnException;

    /**
     * Drops the given index.
     *
     * @param indexId Id of the index to delete
     * @throws NoSuchIndexException Index with supplied index id does not exist.
     */
    void dropIndex(int indexId) throws NoSuchIndexException;

    /**
     * Returns a collection of all indexes on the given column.
     *
     * @param keyColumnId The id of the column on which the indexes are created.
     * @return Matching indexes, empty collection if no index found for the
     *         column.
     * @throws NoSuchColumnException There exists no column with the supplied keyColumnId in the
     *                               table.
     */
    @NotNull
    Collection<Index> getIndexes(int keyColumnId)
            throws NoSuchColumnException;

    /**
     * @return collection of all existing indexes for any columns in that table
     */
    @NotNull
    Collection<Index> getIndexes();

    /**
     * @param indexId id of the requested index
     * @return the index with the given id
     * @throws NoSuchIndexException Index with supplied index id does not exist.
     */
    @NotNull
    Index getIndex(int indexId) throws NoSuchIndexException;
}
