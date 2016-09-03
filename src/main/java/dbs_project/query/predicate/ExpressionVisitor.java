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
 * Interface for a visitor to process predicates.
 * <p/>
 * (http://en.wikipedia.org/wiki/Visitor_pattern)
 */
public interface ExpressionVisitor {

    /**
     * Visit the given expression.
     *
     * @param expression the expression the visit next
     */
    void visitExpression(@NotNull Expression expression);

    /**
     * Visit the given constant.
     *
     * @param constant the constant the visit next
     */
    void visitConstant(@NotNull Constant constant);
}
