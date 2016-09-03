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

import java.io.IOException;
import org.apache.commons.collections.primitives.IntIterator;

/**
 *
 */
public final class IntIteratorWrapper implements IdCursor {

    public IntIteratorWrapper(IntIterator delegate) {
        this.delegate = delegate;
    }

    private final IntIterator delegate;

    private int id = -1;

    @Override
    public boolean next() {
        if (delegate.hasNext()) {
            id = delegate.next();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int getId() {
        return id;
    }

    public static IntIteratorWrapper wrap(IntIterator toWrap) {
        return new IntIteratorWrapper(toWrap);
    }

    @Override
    public void close() throws IOException {
        //ignore
    }
}
