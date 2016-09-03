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

import dbs_project.exceptions.InvalidKeyException;
import dbs_project.exceptions.InvalidRangeException;
import dbs_project.exceptions.RangeQueryNotSupportedException;
import dbs_project.storage.RowCursor;
import dbs_project.util.IdCursor;
import dbs_project.util.annotation.NotNull;
import dbs_project.util.annotation.Nullable;

/**
 * Index for one column of a table.
 * <p/>
 * The index should reflect the data in the column and any chances to that data
 * (your implementation has to synchronize with the column somehow on inserts,
 * updates, deletes,...).
 * <p/>
 * The passed keys are of type Object, so it could represent ever possible type
 * that our database supports. We will not query the key <null> for Integer,
 * Double and Boolean.
 * <p/>
 * The index must be dropped automatically if its column or table is dropped.
 */
public interface Index {

    //Minimal bound for a range query
    public static final Object MINIMUM_SEARCH_KEY = new Object();
    //Maximal bound for a range query
    public static final Object MAXIMUM_SEARCH_KEY = new Object();

    /**
     * Performs a point query and retrieves all rows ids from the table for rows
     * that match supplied search key.
     *
     * @param searchKey Attribute value for which to search
     * @return A cursor containing all matching rows.
     * @throws InvalidKeyException Supplied search key is not valid for the index attribute.
     */
    @NotNull
    RowCursor pointQuery(@Nullable Object searchKey)
            throws InvalidKeyException;

    /**
     * Retrieves all rows from the table matching the supplied range of search
     * keys.
     * It is possible to include (>=, <=) or exclude
     * (>, <) the boundary values of the range.
     *
     * @param startSearchKey  Attribute value where search starts.
     * @param endSearchKey    Attribute value where search ends.
     * @param includeStartKey Include or exclude startKey?
     * @param includeEndKey   Include or exclude endKey?
     * @return A cursor containing all matching rows in the order
     *         corresponding to the index attribute.
     * @throws InvalidRangeException Supplied range is invalid. A range is invalid when the
     *                               startSearchKey is greater than the endSearchKey.
     * @throws InvalidKeyException   Supplied search key types are not valid for the index attribute.
     * @throws RangeQueryNotSupportedException
     *                               The index does not support range queries.
     */
    @NotNull
    RowCursor rangeQuery(@Nullable Object startSearchKey,
                         @Nullable Object endSearchKey, boolean includeStartKey,
                         boolean includeEndKey)
            throws InvalidRangeException, InvalidKeyException,
            RangeQueryNotSupportedException;

    /**
     * Performs a point query and retrieves all rows ids from the table for rows
     * that match supplied search key.
     *
     * @param searchKey Attribute value for which to search
     * @return An iterator containing all ids of the matching rows
     * @throws InvalidKeyException Supplied search key is not valid for the index attribute.
     */
    @NotNull
    IdCursor pointQueryRowIds(@Nullable Object searchKey)
            throws InvalidKeyException;

    /**
     * Retrieves all rows ids from the table matching the supplied range of
     * search keys.
     * <p/>
     * It is possible to include (>=, <=) or exclude
     * (>, <) the boundary values of the range.
     *
     * @param startSearchKey  Attribute value where search starts.
     * @param endSearchKey    Attribute value where search ends.
     * @param includeStartKey Include or exclude startKey?
     * @param includeEndKey   Include or exclude endKey?
     * @return An iterator containing all ids of the matching rows in the order
     *         corresponding to the index attribute.
     * @throws InvalidRangeException Supplied range is invalid. A range is invalid when the
     *                               startSearchKey is greater than the endSearchKey.
     * @throws InvalidKeyException   Supplied search key types are not valid for the index attribute.
     * @throws RangeQueryNotSupportedException
     *                               The index does not support range queries.
     */
    @NotNull
    IdCursor rangeQueryRowIds(@Nullable Object startSearchKey,
                              @Nullable Object endSearchKey, boolean includeStartKey,
                              boolean includeEndKey)
            throws InvalidRangeException, InvalidKeyException,
            RangeQueryNotSupportedException;

    /**
     * @return meta information about the index
     */
    @NotNull
    IndexMetaInfo getIndexMetaInfo();
}
