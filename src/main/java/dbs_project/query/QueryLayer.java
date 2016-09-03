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

package dbs_project.query;

import dbs_project.exceptions.QueryExecutionException;
import dbs_project.query.statement.*;
import dbs_project.storage.Relation;
import dbs_project.util.annotation.NotNull;

/**
 * This interface represents the query layer of the database. The query layer
 * sits on top of the storage and index layer and is a high level interface
 * that is very close to the user.
 * <p/>
 * For example it could sit right behind a SQL parser (feel free to write one if
 * you want ;-)) that takes the user input, creates that the different
 * statement objects and calls the query layer methods.
 * <p/>
 * As a consequence, value literals are passed to this layer as strings and have
 * to be parsed to different types by the system.
 * <p/>
 * The layer is a facade to the systems functionality and provides the
 * different methods for dml, ddl and queries. Behind this facade, your code
 * has to (or in some cases can) do things like selecting rows by evaluating
 * predicates against the data, join processing, query optimization,
 * index creation...
 * <p/>
 * For good performance you should (among other things) think about meaningful
 * physical operators, a simple cost model, a simple query optimizer and access
 * path selection.
 */
public interface QueryLayer {

    /**
     * Execute a query against the database and return the result relation.
     * <p/>
     * IMPORTANT:
     * - multiple table names indicate a join
     * - if a column name in the result relation is not unique, it has to
     * deliver a column label in form: table_name.column_name
     * - same for column names in the query statement (see QueryStatement)
     * - meta information of cursors from the result relation may deliver null
     * on getSourceTable
     * - for joins you only have to support equivalence-joins!
     * - value literals in predicates come as string but are always bound to
     * columns, so you need to parse them to correct type.
     *
     * @param queryStmnt query statement to execute
     * @return result relation for the given query
     * @throws QueryExecutionException
     */
    @NotNull
    Relation executeQuery(@NotNull QueryStatement queryStmnt) throws QueryExecutionException;

    /**
     * Update rows in the database that match a given predicate with given values.
     * <p/>
     * IMPORTANT: values are passed as string and need to be parsed to correct type.
     *
     * @param updateStmnt update statement to execute
     * @return number of updated rows.
     * @throws QueryExecutionException
     */
    int executeUpdateRows(@NotNull UpdateRowsStatement updateStmnt) throws QueryExecutionException;

    /**
     * Delete rows from the database that match a given predicate.
     *
     * @param deleteStmnt delete statement to execute
     * @return number of deleted rows.
     * @throws QueryExecutionException
     */
    int executeDeleteRows(@NotNull DeleteRowsStatement deleteStmnt) throws QueryExecutionException;

    /**
     * Insert rows into the database.
     * <p/>
     * IMPORTANT: values are passed as string and need to be parsed to correct type.
     *
     * @param insertStmnt insert statement to execute
     * @throws QueryExecutionException
     */
    void executeInsertRows(@NotNull InsertRowsStatement insertStmnt) throws QueryExecutionException;

    /**
     * Create a new table.
     *
     * @param createTableStmnt create table statement to execute
     * @throws QueryExecutionException
     */
    void createTable(@NotNull CreateTableStatement createTableStmnt) throws QueryExecutionException;

    /**
     * Create a new column in an existing table.
     *
     * @param createColumnStmnt create column statement to execute
     * @throws QueryExecutionException
     */
    void createColumn(@NotNull CreateColumnStatement createColumnStmnt) throws QueryExecutionException;

    /**
     * Create a new index on an existing column.
     *
     * @param createIndexStmnt create index statement to execute
     * @throws QueryExecutionException
     */
    void createIndex(@NotNull CreateIndexStatement createIndexStmnt) throws QueryExecutionException;

    /**
     * Drop a table from the database.
     *
     * @param dropTableStmnt drop table statement to execute
     * @throws QueryExecutionException
     */
    void dropTable(@NotNull DropTableStatement dropTableStmnt) throws QueryExecutionException;

    /**
     * Drop a column from a table.
     *
     * @param dropColumnStmnt drop column statement to execute
     * @throws QueryExecutionException
     */
    void dropColumn(@NotNull DropColumnStatement dropColumnStmnt) throws QueryExecutionException;

    /**
     * Drop an index from a column.
     *
     * @param dropIndexStmnt drop index statement to execute
     * @throws QueryExecutionException
     */
    void dropIndex(@NotNull DropIndexStatement dropIndexStmnt) throws QueryExecutionException;

    /**
     * Rename a table.
     *
     * @param renameTableStmnt rename table statement to execute
     * @throws QueryExecutionException
     */
    void renameTable(@NotNull RenameTableStatement renameTableStmnt) throws QueryExecutionException;

    /**
     * Rename a column.
     *
     * @param renameColumnStmnt rename column statement to execute
     * @throws QueryExecutionException
     */
    void renameColumn(@NotNull RenameColumnStatement renameColumnStmnt) throws QueryExecutionException;
}
