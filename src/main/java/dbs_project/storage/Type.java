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

package dbs_project.storage;

import java.util.Date;

/**
 * This enum describes all data types you have to support in the project.
 * <p/>
 * Note: you have to distinguish between null-value representation and null for
 * primitive types. For example the value 0 for an int could mean 0 or null.
 * (See GenericLinearReadAccessible.isNull(int position))
 * <p/>
 * Hint: You can use enums in switch statements.
 */
public enum Type {
    //The supported types
    INTEGER(int.class),
    DOUBLE(double.class),
    STRING(String.class),
    DATE(Date.class),
    BOOLEAN(boolean.class);
    //The null value representation for primitive types.
    public static final int NULL_VALUE_INTEGER = 0;
    public static final double NULL_VALUE_DOUBLE = 0.0d;
    public static final boolean NULL_VALUE_BOOLEAN = false;
    //
    private final Class<?> javaClass;

    private Type(Class<?> wrapperClass) {
        this.javaClass = wrapperClass;
    }

    /**
     * @return corresponding java class for the type
     */
    public Class<?> getJavaClass() {
        return javaClass;
    }
}
