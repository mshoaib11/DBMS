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

import dbs_project.exceptions.ColumnAlreadyExistsException;
import dbs_project.exceptions.NoSuchColumnException;
import dbs_project.exceptions.NoSuchRowException;
import dbs_project.exceptions.SchemaMismatchException;
import dbs_project.util.IdCursor;
import dbs_project.util.annotation.NotNull;

/**
 * Interface for a table in the database.
 */
public interface Table extends Relation {

    /**
     * Sets the name of specified column
     *
     * @param columnId Unique id of the column to rename
     * @param newColumnName New name
     * @throws ColumnAlreadyExistsException ColumnAlreadyExistsException
     * @throws NoSuchColumnException NoSuchColumnException
     */
    void renameColumn(int columnId, @NotNull String newColumnName)
            throws ColumnAlreadyExistsException, NoSuchColumnException;

    /**
     * Add a new, empty column to a table. Values are initialized as null.
     *
     * @param columnName Column name
     * @param columnType Column type
     * @return column id that is unique within this table. Can be used to
     * address the column in the context of the table.
     * @throws ColumnAlreadyExistsException Column with the same name already
     * exists in the table
     */
    int createColumn(@NotNull String columnName, @NotNull Type columnType)
            throws ColumnAlreadyExistsException;

    /**
     * Inserts a row in this table. Copies the row values into columns with
     * matching names.
     *
     * @param row New row to add
     * @return Row Id that is unique in for this table (can be useful for
     * indexing later on)
     * @throws SchemaMismatchException Schema of supplied row does not match
     * that of table
     */
    int addRow(@NotNull Row row) throws SchemaMismatchException;

    /**
     * Inserts rows in this table. Copies the row values into columns with
     * matching names.
     *
     * @param rows New rows to add.
     * @param meta row meta data used to insert all rows
     * @return Cursor of Row Ids (unique RowIds for this table, can be useful
     * for indexing later on).
     * @throws SchemaMismatchException Schema of supplied row does not match
     * that of table
     */
    @NotNull
    IdCursor addRows(@NotNull RowMetaData meta, @NotNull RowCursor rows) throws SchemaMismatchException;

    /**
     * Inserts a column in this table, named and typed as described in its meta
     * data.
     * <p/>
     * Number of values in the column have to match the number of rows in the
     * table, if the table is not empty.
     *
     * @param column New column
     * @return Column Id that is unique for this table (can be useful for
     * indexing later on)
     * @throws SchemaMismatchException Schema of supplied column does not match
     * that of table
     * @throws ColumnAlreadyExistsException Column with the same name already
     * exists in the table
     */
    int addColumn(@NotNull Column column) throws SchemaMismatchException, ColumnAlreadyExistsException;

    /**
     * Inserts columns in this table, named and typed as described in their meta
     * data.
     * <p/>
     * Number of values in all columns have to match the number of rows in the
     * table, if the table is not empty.
     *
     * @param columns New columns
     * @return Column Ids that are unique in for this table (can be useful for
     * indexing later on)
     * @throws SchemaMismatchException Schema of at least one supplied column
     * does not match that of table
     * @throws ColumnAlreadyExistsException Column with the same name already
     * exists in the table
     */
    @NotNull
    IdCursor addColumns(@NotNull ColumnCursor columns) throws SchemaMismatchException, ColumnAlreadyExistsException;

    /**
     * Delete matching rows
     *
     * @param rowId Id of row to be deleted
     * @throws NoSuchRowException DeprecatedRow does not exist
     */
    void deleteRow(int rowId) throws NoSuchRowException;

    /**
     * Delete matching rows
     *
     * @param rowIds Ids of rows to be deleted
     * @throws NoSuchRowException DeprecatedRow does not exist
     */
    void deleteRows(@NotNull IdCursor rowIds) throws NoSuchRowException;

    /**
     * Drop a column
     *
     * @param columnId Id of the column to drop
     * @throws NoSuchColumnException NoSuchColumn
     */
    void dropColumn(int columnId)
            throws NoSuchColumnException;

    /**
     * Drop a column
     *
     * @param columnId Id of the column to drop
     * @throws NoSuchColumnException NoSuchColumn
     */
    void dropColumns(@NotNull IdCursor columnIds)
            throws NoSuchColumnException;

    /**
     * Get a reference to a column
     *
     * @param columnId Id of the column
     * @return A reference to the column
     * @throws NoSuchColumnException The requested column does not exist
     */
    @NotNull
    Column getColumn(int columnId)
            throws NoSuchColumnException;

    /**
     * @return Collection of all column instances in this table with the given
     * Ids
     * @throws NoSuchColumnException One of the requested columns does not exist
     */
    @NotNull
    ColumnCursor getColumns(@NotNull IdCursor columnIds) throws NoSuchColumnException;

    /**
     * @param rowIds The ids of the requested rows
     * @return An operator that supplies the rows
     * @throws NoSuchRowException A row for on of the given ids does not exist
     */
    @NotNull
    RowCursor getRows(@NotNull IdCursor rowIds) throws NoSuchRowException;

    /**
     * @param rowId id of the requested row
     * @return the requested row
     * @throws NoSuchRowException A row for the given id does not exist
     */
    @NotNull
    Row getRow(int rowId) throws NoSuchRowException;

    /**
     * Update a row with matching id using supplied values
     *
     * @param rowId If of row to be updated
     * @param newRow New row
     * @throws SchemaMismatchException Schema of supplied row does not match
     * that of table
     * @throws NoSuchRowException Referenced row does not exist
     */
    void updateRow(int rowId, @NotNull Row newRow)
            throws SchemaMismatchException, NoSuchRowException;

    /**
     * Update rows with matching Ids using supplied rows. #ids == #rows!
     *
     * @param meta row meta data used to insert all rows
     * @param rowIds Ids of rows to be updated
     * @param newRows New rows
     * @throws SchemaMismatchException Schema of supplied rows does not match
     * that of table
     * @throws NoSuchRowException Referenced row does not exist
     */
    void updateRows(@NotNull RowMetaData meta, @NotNull IdCursor rowIds, @NotNull RowCursor newRows)
            throws SchemaMismatchException, NoSuchRowException;

    /**
     * OPTIONAL METHOD. YOU DON'T HAVE TO IMPLEMENT THIS!
     *
     * Update columns with matching Ids using supplied columns. #ids == #columns!
     * <p/>
     * Number of values in the update columns have to match the number of values
     * in the other columns, if other columns exist.
     *
     * @param columnIds Ids of columns to be updated
     * @param updateColumns New columns
     * @throws SchemaMismatchException Schema of supplied rows does not match
     * that of table
     * @throws NoSuchColumnException Referenced column does not exist
     */
    void updateColumns(@NotNull IdCursor columnIds, @NotNull ColumnCursor updateColumns)
            throws SchemaMismatchException, NoSuchColumnException;

    /**
     * OPTIONAL METHOD. YOU DON'T HAVE TO IMPLEMENT THIS!
     *
     * Update a complete column with matching Id using supplied values.
     * <p/>
     * Number of values in the update column have to match the number of values
     * in the other columns, if other columns exist.
     *
     * @param columnId Id of column to be updated
     * @param updateColumn New column
     * @throws SchemaMismatchException Schema of supplied column does not match
     * that of table
     * @throws NoSuchColumnException Referenced column does not exist
     */
    void updateColumn(int columnId, @NotNull Column updateColumn)
            throws SchemaMismatchException, NoSuchColumnException;

    /**
     * @return Meta data that describes the table
     */
    @NotNull
    TableMetaData getTableMetaData();
}
