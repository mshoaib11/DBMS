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

package dbs_project.query.predicate.impl;

import dbs_project.query.predicate.Expression;
import dbs_project.query.predicate.ExpressionElement;
import dbs_project.query.predicate.ExpressionVisitor;
import dbs_project.query.predicate.Operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Creates Expression instances through static factory methods.
 */
public final class Expressions {

    private Expressions() {
        throw new AssertionError("fail.");
    }

    /**
     * @param operator
     * @param operands
     * @return n-ary expression from the ellipse
     */
    public static Expression createExpression(Operator operator, ExpressionElement... operands) {
        return new ExpressionImpl(operator, Arrays.asList(operands));
    }

    /**
     * @param operator
     * @param operands
     * @return n-ary expression from the list
     */
    public static Expression createExpression(Operator operator, List<ExpressionElement> operands) {
        return new ExpressionImpl(operator, operands);
    }

    /**
     * @param operator
     * @param lhsOperand left hand side operand
     * @param rhsOperand right hand side operand
     * @return Binary expression
     */
    public static Expression createExpression(Operator operator, ExpressionElement lhsOperand, ExpressionElement rhsOperand) {
        final List<ExpressionElement> operands = new ArrayList<ExpressionElement>();
        operands.add(lhsOperand);
        operands.add(rhsOperand);
        return new ExpressionImpl(operator, operands);
    }

    /**
     * Implementation of Expression interface
     */
    static final class ExpressionImpl implements Expression {

        /**
         * @param operator
         * @param operands
         */
        public ExpressionImpl(Operator operator, List<ExpressionElement> operands) {
            this.operator = operator;
            this.operands = operands;
        }

        private final Operator operator;
        private final List<ExpressionElement> operands;

        @Override
        public void addOperand(ExpressionElement operand) {
            operands.add(operand);
        }

        @Override
        public void removeOperand(int position) {
            this.operands.remove(position);
        }

        @Override
        public Operator getOperator() {
            return operator;
        }

        @Override
        public ExpressionElement getOperand(int position) {
            return operands.get(position);
        }

        @Override
        public int getOperandCount() {
            return operands.size();
        }

        @Override
        public void accept(ExpressionVisitor visitor) {
            visitor.visitExpression(this);
        }

        @Override
        public void addOperand(int position, ExpressionElement operand) {
            operands.add(position, operand);
        }

        @Override
        public String toString() {
            final StringBuilder result = new StringBuilder();
            result.append('(');
            for (int i = 0; i < operands.size(); ++i) {
                final ExpressionElement subExp = operands.get(i);
                if (i > 0) {
                    result.append(' ').append(operator).append(' ');
                }
                result.append(subExp);
            }
            result.append(')');
            return result.toString();
        }

        //generated stuff
        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ExpressionImpl other = (ExpressionImpl) obj;
            if (this.operator != other.operator) {
                return false;
            }
            if (this.operands != other.operands && (this.operands == null || !this.operands.equals(other.operands))) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 97 * hash + (this.operator != null ? this.operator.hashCode() : 0);
            hash = 97 * hash + (this.operands != null ? this.operands.hashCode() : 0);
            return hash;
        }
    }
}
