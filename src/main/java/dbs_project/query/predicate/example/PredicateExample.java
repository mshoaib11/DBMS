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
import dbs_project.query.predicate.Operator;
import dbs_project.query.predicate.impl.Constants;
import dbs_project.query.predicate.impl.Expressions;

import java.util.ArrayList;

/**
 * Example code for predicates.
 */
public class PredicateExample {

    public static void main(String[] args) {
        //make a predicate expression
        final Expression expCompareEquals = Expressions.createExpression(Operator.EQ, new ArrayList<ExpressionElement>());
        final Expression expCompareGreaterEquals = Expressions.createExpression(Operator.GEQ, new ArrayList<ExpressionElement>());
        final Expression expCompareLess = Expressions.createExpression(Operator.LT, new ArrayList<ExpressionElement>());
        final Expression expOr = Expressions.createExpression(Operator.OR, new ArrayList<ExpressionElement>());
        final Expression expRootAnd = Expressions.createExpression(Operator.AND, new ArrayList<ExpressionElement>());
        expRootAnd.addOperand(expCompareEquals);
        expRootAnd.addOperand(expOr);
        expOr.addOperand(expCompareGreaterEquals);
        expCompareEquals.addOperand(Constants.createColumnNameConstant("table_1.column_xzy"));
        expCompareEquals.addOperand(Constants.createLiteralConstant("test_string"));
        final Constant constColumnName = Constants.createColumnNameConstant("table_2.test_column");
        expCompareGreaterEquals.addOperand(constColumnName);
        expCompareGreaterEquals.addOperand(Constants.createLiteralConstant("123"));
        expCompareLess.addOperand(constColumnName);
        expCompareLess.addOperand(Constants.createLiteralConstant("42"));
        expOr.addOperand(expCompareLess);
        //expression visitor
        final ExpressionVisitorExampleImpl expVisitor = new ExpressionVisitorExampleImpl();
        expRootAnd.accept(expVisitor);
        System.out.println(expVisitor);
    }
}
