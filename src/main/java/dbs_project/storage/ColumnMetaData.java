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

import dbs_project.util.Identifiable;
import dbs_project.util.Named;
import dbs_project.util.annotation.NotNull;
import dbs_project.util.annotation.Nullable;

/**
 * Provides information and properties for a column.
 * Name and Id have to be at least unique within the source table.
 * In other words, a column can be identified by a composite key of the owning
 * table and column id (prefered for internal identification) or owning table 
 * and column name (external, e.g. when inserting new user data via row cursor 
 * or answering user queries)
 */
public interface ColumnMetaData extends Identifiable, Named {

    /**
     * @return number of values in this column (if known or negative value otherwise)
     */
    int getRowCount();

    /**
     * @return the table this column's data originated from - if applicable
     */
    @Nullable
    Table getSourceTable();

    /**
     * @return Label for this column or source table name + . + column name,
     *         if the column was not explicitly labeled (like in: select xyz as label ...)
     */
    @NotNull
    String getLabel();

    /**
     * @return Type of the values in this column
     */
    @NotNull
    Type getType();


    /**
     * Translates the column index for getXXX access methods to a row id
     * E.g. this is needed for tuple reconstruction when using add/updateColumn()
     *
     * @param positionInColumn [0, #rows-1]
     * @return the id of the row that the value at the given position belongs to.
     */
    int getRowId(int positionInColumn) throws IndexOutOfBoundsException;

}
