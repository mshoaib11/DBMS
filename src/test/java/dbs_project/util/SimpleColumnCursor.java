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

import com.google.common.base.Preconditions;
import dbs_project.storage.ColumnCursor;
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.Table;
import dbs_project.storage.Type;
import java.io.IOException;

import java.util.Date;
import java.util.List;

/**
 * Implementation of a ColumnCursor for SimpleColumns that we use for our tests.
 */
public final class SimpleColumnCursor implements ColumnCursor, ColumnMetaData {

    private final List<SimpleColumn> columns;
    private SimpleColumn current;
    private int position;

    public SimpleColumnCursor(List<SimpleColumn> columns) {
        Preconditions.checkNotNull(columns);
        this.columns = columns;
        this.position = -1;
    }


    @Override
    public ColumnMetaData getMetaData() {
        return current.getMetaData();
    }

    @Override
    public int getInteger(int index) throws IndexOutOfBoundsException, ClassCastException {
        return isNull(index) ? Type.NULL_VALUE_INTEGER : current.getInteger(index);
    }

    @Override
    public boolean getBoolean(int index) throws IndexOutOfBoundsException, ClassCastException {
        return isNull(index) ? Type.NULL_VALUE_BOOLEAN : current.getBoolean(index);
    }

    @Override
    public double getDouble(int index) throws IndexOutOfBoundsException, ClassCastException {
        return isNull(index) ? Type.NULL_VALUE_DOUBLE : current.getDouble(index);
    }

    @Override
    public Date getDate(int index) throws IndexOutOfBoundsException, ClassCastException {
        return current.getDate(index);
    }

    @Override
    public String getString(int index) throws IndexOutOfBoundsException {
        return isNull(index) ? null : String.valueOf(current.getObject(index));
    }

    @Override
    public Object getObject(int index) throws IndexOutOfBoundsException {
        return current.getObject(index);
    }

    @Override
    public boolean isNull(int index) throws IndexOutOfBoundsException {
        return current.isNull(index);
    }

    @Override
    public boolean next() {
        ++position;
        if (position < columns.size()) {
            current = columns.get(position);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int getRowCount() {
        return current.getRowCount();
    }

    @Override
    public Table getSourceTable() {
        return current.getSourceTable();
    }

    @Override
    public String getLabel() {
        return current.getLabel();
    }

    @Override
    public Type getType() {
        return current.getType();
    }

    @Override
    public int getRowId(int positionInColumn) throws IndexOutOfBoundsException {
        return current.getRowId(positionInColumn);
    }

    @Override
    public int getId() {
        return current.getId();
    }

    @Override
    public String getName() {
        return current.getName();
    }

    @Override
    public void close() throws IOException {
        //ignore
    }
}
