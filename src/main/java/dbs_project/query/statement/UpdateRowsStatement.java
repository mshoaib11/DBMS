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
import dbs_project.query.statement.elements.StatementElementPredicate;
import dbs_project.query.statement.elements.StatementElementTable;
import dbs_project.util.annotation.NotNull;

import java.util.List;

/**
 * Update statement. Update all rows from the table with the given name that
 * fulfill the predicate using the provided row data.
 * <p/>
 * See InsertRowsStatement for more information.
 */
public interface UpdateRowsStatement extends StatementElementTable, StatementElementPredicate, StatementElementMultiColumn {

    /**
     * @return list of data in an order that matches the column names.
     */
    @NotNull
    List<String> getUpdateRowData();
}
