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

import java.util.Map;

/**
 * Provides information and properties for a table.
 */
public interface TableMetaData extends Identifiable, Named {

    /**
     * @return the schema for the table (column name -> column meta data, no ordering assumed)
     */
    @NotNull
    Map<String, ColumnMetaData> getTableSchema();

    /**
     * @return number of rows in the table if known, or negative value otherwise
     */
    int getRowCount();

}
