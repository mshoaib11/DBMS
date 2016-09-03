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

import dbs_project.query.predicate.Constant;
import dbs_project.query.predicate.ExpressionVisitor;

import static dbs_project.query.predicate.Constant.ConstantType.*;

/**
 * Creates Constant instances through static factory methods.
 */
public final class Constants {

    private static final Constant NULL_CONSTANT = new ConstantImpl("NULL", NULL_LITERAL);

    private Constants() {
        throw new AssertionError("fail.");
    }

    /**
     * @param columnName
     * @return constant of type COLUMN_NAME
     */
    public static Constant createColumnNameConstant(String columnName) {
        return new ConstantImpl(columnName, COLUMN_NAME);
    }

    /**
     * @param literalValue
     * @return constant of type VALUE_LITERAL
     */
    public static Constant createLiteralConstant(String literalValue) {
        return new ConstantImpl(literalValue, VALUE_LITERAL);
    }

    /**
     * @return constant of type NULL_CONSTANT
     */
    public static Constant createNullConstant() {
        return NULL_CONSTANT;
    }

    /**
     * Implementation of the Constant interface
     */
    static final class ConstantImpl implements Constant {

        private final String value;
        private final ConstantType constantType;

        public ConstantImpl(String value, ConstantType constantType) {
            this.value = value;
            this.constantType = constantType;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public ConstantType getType() {
            return constantType;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        @Override
        public void accept(ExpressionVisitor visitor) {
            visitor.visitConstant(this);
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
            final ConstantImpl other = (ConstantImpl) obj;
            if ((this.value == null) ? (other.value != null) : !this.value.equals(other.value)) {
                return false;
            }
            if (this.constantType != other.constantType) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 53 * hash + (this.value != null ? this.value.hashCode() : 0);
            hash = 53 * hash + (this.constantType != null ? this.constantType.hashCode() : 0);
            return hash;
        }
    }
}
