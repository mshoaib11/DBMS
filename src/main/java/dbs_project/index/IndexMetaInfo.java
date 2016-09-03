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

import dbs_project.storage.Column;
import dbs_project.util.Identifiable;
import dbs_project.util.Named;

/**
 * Meta information about an index.
 */
public interface IndexMetaInfo extends Identifiable, Named {

    /**
     * @return the table which contains the key column for the index
     */
    IndexableTable getTable();

    /**
     * @return the key column for the index
     */
    Column getKeyColumn();

    /**
     * @return cardinality of the index, including null values - if 
     *         allowed (= number of different values in the key 
     *         column)
     */
    int getKeyCount();

    /**
     * @return type of the index
     */
    IndexType getIndexType();

    /**
     * Does the supplied index support range queries?
     *
     * @return indication whether or not the index supports range queries.
     */
    boolean supportsRangeQueries();
}
