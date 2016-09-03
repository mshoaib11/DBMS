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
import dbs_project.query.statement.elements.StatementElementMultiTable;
import dbs_project.query.statement.elements.StatementElementPredicate;

/**
 * Query statement. Must support qualified column names.
 * <p/>
 * - column names ~ select clause.
 * - tables names ~ from clause.
 * - predicate ~ where clause.
 * <p/>
 * IMPORTANT NOTES AND RESTRICTION:
 * <p/>
 * - We restrict allowed joins for our tests to equivalence-joins only!
 * - Column names that are not unique between all given tables must be passed as
 * full qualified names in format: table_name.column_name (exception otherwise).
 */
public interface QueryStatement extends StatementElementMultiTable, StatementElementMultiColumn, StatementElementPredicate {

}
