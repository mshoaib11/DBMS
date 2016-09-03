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

import dbs_project.index.IndexType;
import dbs_project.query.statement.elements.StatementElementIndex;
import dbs_project.util.annotation.NotNull;

/**
 * Create index statement. crete an index with the given name and type on the
 * given column of the given table.
 */
public interface CreateIndexStatement extends StatementElementIndex {

    /**
     * @return type of the index to create.
     */
    @NotNull
    IndexType getIndexType();
}
