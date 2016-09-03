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

package dbs_project.query.predicate;

import dbs_project.util.annotation.NotNull;

/**
 * A constant in a predicate expression. All values are represented as strings
 * (like a sql parser would provide them) and eventually need to be parsed for usage.
 * <p/>
 * In a composite pattern (http://en.wikipedia.org/wiki/Composite_pattern), this
 * is the "leaf" interface (Constants are the leafs in the expression composite tree).
 * <p/>
 * COLUMN_NAME: this value represents a column name.
 * <p/>
 * VALUE_LITERAL: this value represents a not null literal like a string, date or int.
 * <p/>
 * NULL_LITERAL: this value represents null.
 */
public interface Constant extends ExpressionElement {

    enum ConstantType {
        COLUMN_NAME, VALUE_LITERAL, NULL_LITERAL
    }

    /**
     * @return string representation of the constants value
     */
    @NotNull
    String getValue();

    /**
     * @return type of this constant
     */
    @NotNull
    ConstantType getType();

}
