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

package dbs_project.exceptions;

/**
 * Thrown if data is to be inserted that somehow violates the table structure.
 * <p/>
 * (e.g. columns or row count not matching)
 */
public class SchemaMismatchException extends Exception {

    /**
     *
     */
    public SchemaMismatchException() {
        super();
    }

    /**
     * @param message
     * @param cause
     */
    public SchemaMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     */
    public SchemaMismatchException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public SchemaMismatchException(Throwable cause) {
        super(cause);
    }
}
