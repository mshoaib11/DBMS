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
 * Thrown if the persistence layer tries to begin a new transaction while
 * there is still a transaction running.
 */
public class TransactionAlreadyActiveException extends Exception {


    /**
     *
     */
    public TransactionAlreadyActiveException() {
        super();
    }

    /**
     * @param message
     * @param cause
     */
    public TransactionAlreadyActiveException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * @param message
     */
    public TransactionAlreadyActiveException(String message) {
        super(message);
    }

    /**
     * @param cause
     */
    public TransactionAlreadyActiveException(Throwable cause) {
        super(cause);
    }
}
