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

package dbs_project.query.statement;

import dbs_project.query.statement.elements.StatementElementMultiColumn;
import dbs_project.query.statement.elements.StatementElementTable;
import dbs_project.util.annotation.NotNull;

import java.util.Iterator;
import java.util.List;

/**
 * Insert rows statement. Insert the row data into the table with the given name.
 * <p/>
 * Column names and data come as lists of string (like a sql parser would
 * create from the VALUES clause, column_name[i] refers to column_data[i]).
 * <p/>
 * Hence, the data has to be parsed to the right types by your system
 * (with e.g. Integer.parseInt(...), etc ).
 * <p/>
 * Date format should be parsed with java.sql.Date.valueOf (...).
 */
public interface InsertRowsStatement extends StatementElementTable, StatementElementMultiColumn {

    /**
     * @return iterator for lists of data. data is in an order that matches the column names order.
     */
    @NotNull
    Iterator<List<String>> getDataForRows();
}
