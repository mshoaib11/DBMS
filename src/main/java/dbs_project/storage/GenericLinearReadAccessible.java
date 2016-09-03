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

import dbs_project.util.annotation.Nullable;

import java.util.Date;

/**
 * This interface allows read access to linear data (array, column, row).
 * It allows access to primitive types without autoboxing.
 * <p/>
 * Valid Java casts (e.g. double -> int) should be supported
 * without throwing a ClassCastException. However illegal casts (like int -> Date)
 * should result in a ClassCastException.
 * <p/>
 * Access to invalid indexes should throw an IndexOutOfBoundsException.
 * <p/>
 * Although theses are all RuntimeExceptions (for convenience in later use) and
 * the compiler will not force you to implement them, we will test correct behaviour.
 */
public interface GenericLinearReadAccessible {

    /**
     * @param index - the first index is 0, the second is 1, ...
     * @return the value on this position; if the value is NULL, the value returned is Type.NULL_VALUE_INTEGER
     */
    int getInteger(int index) throws IndexOutOfBoundsException, ClassCastException;

    /**
     * @param index - the first index is 0, the second is 1, ...
     * @return the value on this position; if the value is NULL, the value returned is Type.NULL_VALUE_BOOLEAN
     */
    boolean getBoolean(int index) throws IndexOutOfBoundsException, ClassCastException;

    /**
     * @param index - the first column is 0, the second is 1, ...
     * @return the value on this position; if the value is NULL, the value returned is Type.NULL_VALUE_DOUBLE
     */
    double getDouble(int index) throws IndexOutOfBoundsException, ClassCastException;

    /**
     * @param index - the first index is 0, the second is 1, ...
     * @return the value on this position
     */
    @Nullable
    Date getDate(int index) throws IndexOutOfBoundsException, ClassCastException;

    /**
     * This must work for all types.
     * If value is not instance of string, return it's String representation
     *
     * @param index - the first index is 0, the second is 1, ...
     * @return the value on this position
     */
    @Nullable
    String getString(int index) throws IndexOutOfBoundsException;

    /**
     * this works for all types
     *
     * @param index - the first index is 0, the second is 1, ...
     * @return the value on this position
     */
    @Nullable
    Object getObject(int index) throws IndexOutOfBoundsException;

    /**
     * Use this method to distinguish between null-value representation and null for
     * primitive types. For example the value 0 for an int could mean 0 or null.
     * (see Type)
     * @param index - the first index is 0, the second is 1, ...
     * @return true if the value at this position is NULL and false otherwise
     */
    boolean isNull(int index) throws IndexOutOfBoundsException;
}
