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

package dbs_project.query.statement.elements;

import dbs_project.util.annotation.NotNull;

import java.util.List;

/**
 * Part of a statement that delivers row data. (like for inserts).
 * Column names and data come as lists of string (like a sql parser would
 * create from the VALUES clause). Hence, the data has to be parsed to the
 * right types by your system (with e.g. Integer.parseInt(...), etc ).
 * <p/>
 * Date format should be parsed with java.sql.Date.valueOf (...).
 * <p/>
 * column_name[i] <-> column_data[i].
 */
public interface StatementElementRowData extends StatementElementMultiColumn {

    /**
     * @return list of data in an order that matches the column names.
     */
    @NotNull
    List<String> getRowData();
}
