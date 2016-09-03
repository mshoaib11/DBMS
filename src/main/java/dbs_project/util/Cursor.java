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

package dbs_project.util;

import java.io.Closeable;

/**
 * Interface for an object that allows iteration over a sequence by
 * changing it's internal values to represent the next element when next()
 * is called.
 * Cursors start one position *before* the first item (-1) and stop one
 * position *behind* the last.
 * ([-1, #items], first valid item has position 0).
 */
public interface Cursor extends Closeable {

    /**
     * Moves the cursor to the next position.
     *
     * @return true if the cursor was moved to a valid position
     *         ([0, #item - 1])
     */
    boolean next();

}
