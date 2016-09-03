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

package dbs_project.persistence;

import dbs_project.exceptions.NoTransactionActiveException;
import dbs_project.exceptions.TransactionAlreadyActiveException;

/**
 * Simple persistence layer of the database. This layer allows you to write
 * your database to stable storage and restore the state of the database from
 * there.
 * <p/>
 * It supports a simple form of transactions, with only one single active
 * transaction at a time. The properties you have to implement are atomicity and
 * durability:
 * <p/>
 * - Every change by a committed transaction has to be durable.
 * <p/>
 * - If an active transaction is interrupted (e.g. crash, power failure, ...)
 * before committing all changes from that transaction may not reflect in the
 * database after recovery.
 * <p/>
 * - When the database is started, it has to perform crash recovery if
 * necessary.
 * <p/>
 * All changes to the database that are performed without a begin/commit block
 * are interpreted as "autocommit" (= durable).
 */
public interface PersistenceLayer {

    /**
     * Define whether persistence shall be used or not.
     * If set to false durability and atomicity do not have to be guarantied.
     *
     * @param enabled new persistence status (default: false)
     */
    void setPersistence(boolean enabled);

    /**
     * Begin a new transaction. The system only handles one transaction at a
     * time. This throws an exception if there currently is an active
     * transaction.
     *
     * @throws TransactionAlreadyActiveException
     *
     */
    void beginTransaction() throws TransactionAlreadyActiveException;

    /**
     * Commit the current transaction. Committed transactions have to be durable
     * (e.g. to "survive" crashes). Throws an exception if there is no active
     * transaction.
     *
     * @throws NoTransactionActiveException
     */
    void commitTransaction() throws NoTransactionActiveException;

    /**
     * Abort the current transaction. All changes performed by the transaction
     * have to be undone. Throws an exception if there is no active transaction.
     *
     * @throws NoTransactionActiveException
     */
    void abortTransaction() throws NoTransactionActiveException;

    /**
     * Check if there is an active transaction.
     *
     * @return true if there is an active transaction, false otherwise.
     */
    boolean hasActiveTransaction();

}
