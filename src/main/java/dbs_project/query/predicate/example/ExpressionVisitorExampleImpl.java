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

package dbs_project.query.predicate.example;

import dbs_project.query.predicate.Constant;
import dbs_project.query.predicate.Expression;
import dbs_project.query.predicate.ExpressionElement;
import dbs_project.query.predicate.ExpressionVisitor;

/**
 * Example implementation of ExpressionVisitor for the infix notation of
 * an expression. 
 */
public class ExpressionVisitorExampleImpl implements ExpressionVisitor {

    public ExpressionVisitorExampleImpl() {
        this.result = new StringBuilder();
    }

    private final StringBuilder result;

    @Override
    public void visitExpression(Expression expression) {
        result.append('(');
        for (int i = 0; i < expression.getOperandCount(); ++i) {
            ExpressionElement node = expression.getOperand(i);
            if (i > 0) {
                result.append(' ').append(expression.getOperator()).append(' ');
            }
            node.accept(this);
        }
        result.append(')');
    }

    @Override
    public void visitConstant(Constant constant) {
        result.append(constant.getValue());
    }

    @Override
    public String toString() {
        return result.toString();
    }
}
