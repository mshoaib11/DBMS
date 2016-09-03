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
 * Superclass for all nodes in a predicate tree. Supports Visitor pattern through
 * ExpressionVisitor. (http://en.wikipedia.org/wiki/Visitor_pattern)
 * <p/>
 * In a composite pattern (http://en.wikipedia.org/wiki/Composite_pattern), this
 * is the "component" interface.
 */
public interface ExpressionElement {

    /**
     * Accept method for ExpressionVisitor.
     *
     * @param visitor an expression visitor to traverse expression trees
     */
    void accept(@NotNull ExpressionVisitor visitor);

}
