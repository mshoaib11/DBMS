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
import dbs_project.util.annotation.Nullable;

/**
 * Expression for a predicate.
 * <p/>
 * In a composite pattern (http://en.wikipedia.org/wiki/Composite_pattern), this
 * is the "composite" interface.
 * <p/>
 * Examples:
 * <p/>
 * a) operator = ==, operands = {A, B} --> (A == B)
 * <p/>
 * b) operator = AND, operands = {A, B, C} --> (A AND B AND C)
 * <p/>
 * c) operator = OR, operands = {A, exp(AND, {B, C, D})} --> (A OR (B OR C OR D))
 * <p/>
 * IMPORTANT RESTRICTIONS:
 * <p/>
 * Constants will only appear as operands in comparisons operations (==, <=, >=,
 * <, >), not as operands in AND/ORs. AND/OR will only connect Expressions.
 * <p/>
 * Comparisons will never compare literals against each other, only literals
 * against column values (so that you can always find the right runtime type for a
 * literal).
 * <p/>
 * Examples for comparisons:
 * <p/>
 * valid:
 * - column_name_1 == column_name_2 //e.g. equi join of 2 tables
 * - column_name_1 == column_name_2 == column_name_3 //e.g. equi join of 3 tables
 * - column_name_1 > column_name_2 // e.g. for update
 * - literal <= column_name // e.g. simple select
 * - literal_1 < column_name < literal_2 // e.g. simple select
 * <p/>
 * invalid:
 * - literal_1 < literal_2
 * - column_name == literal_1 == literal_2
 */
public interface Expression extends ExpressionElement {

    /**
     * @return the operator to apply on all operands
     */
    @NotNull
    Operator getOperator();

    /**
     * @param position position of the requested operand
     * @return the operand at the given position
     */
    @Nullable
    ExpressionElement getOperand(int position);

    /**
     * @return number of operands in this expression
     */
    int getOperandCount();

    /**
     * adds another operand to this expression (maintains order).
     * <p/>
     * example: (A AND B).addOperand(C) --> (A AND B AND C)
     *
     * @param operand operand to add to the expression
     */
    void addOperand(@NotNull ExpressionElement operand);

    /**
     * adds another operand to this expression at the given position. (shifts if position is already occupied)
     * <p/>
     * example: (A OR B).addOperand(1, C) --> (A OR C OR B)
     *
     * @param position position to add the operand in expression
     * @param operand  operand to add to the expression
     */
    void addOperand(int position, @NotNull ExpressionElement operand);

    /**
     * removes the operand at the given position from this expression. (also shifting)
     * <p/>
     * example: (A <= B <= C).removeOperand(A) --> (B <= C)
     *
     * @param position position to add the operand in expression
     */
    void removeOperand(int position);
}
