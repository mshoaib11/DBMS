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
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.RowCursor;
import dbs_project.storage.RowMetaData;
import java.io.IOException;

import java.util.Date;
import java.util.List;

/**
 * Implementation of a RowCursor for SimpleColumns that we use for our tests.
 */
public final class SimpleRowCursor implements RowCursor, RowMetaData {

    private final List<SimpleColumn> columns;
    private int position;
    private int rowCount;

    public SimpleRowCursor(List<SimpleColumn> columns) {
        Preconditions.checkNotNull(columns);
        this.columns = columns;
        this.position = -1;
        if (columns.size() > 0) {
            this.rowCount = columns.get(0).getMetaData().getRowCount();
        } else {
            this.rowCount = 0;
        }
    }
    
    public void setRowCount(int rowCount) {
    	this.rowCount = rowCount;
    }

    @Override
    public RowMetaData getMetaData() {
        return this;
    }

    @Override
    public int getInteger(int index) throws IndexOutOfBoundsException, ClassCastException {
        return columns.get(index).getInteger(position);
    }

    @Override
    public boolean getBoolean(int index) throws IndexOutOfBoundsException, ClassCastException {
        return columns.get(index).getBoolean(position);
    }

    @Override
    public double getDouble(int index) throws IndexOutOfBoundsException, ClassCastException {
        return columns.get(index).getDouble(position);
    }

    @Override
    public Date getDate(int index) throws IndexOutOfBoundsException, ClassCastException {
        return columns.get(index).getDate(position);
    }

    @Override
    public String getString(int index) throws IndexOutOfBoundsException {
        return columns.get(index).getString(position); //position = colNumber... getString from SimpleColumn
    }

    @Override
    public Object getObject(int index) throws IndexOutOfBoundsException {
        return columns.get(index).getObject(position);
    }

    @Override
    public boolean isNull(int index) throws IndexOutOfBoundsException {
        return columns.get(index).isNull(position);
    }

    @Override
    public boolean next() {
        ++position;
        if (position < rowCount) {
            return true;
        } else {
            return false;
        }
    }

    public void reset() {
        position = -1;
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public ColumnMetaData getColumnMetaData(int positionInTheRow) throws IndexOutOfBoundsException {
        return columns.get(positionInTheRow);
    }

    @Override
    public int getId() {
        return position;
    }

    public void deleteCurrentRow() {
        for (SimpleColumn c : columns) {
            c.remove(position);
        }
        --rowCount;
    }

    public void deleteRow(int row) {
        for (SimpleColumn c : columns) {
            c.remove(row);
        }
        --rowCount;
    }

    public void setValue(int index, Object value) {
        columns.get(index).set(position, value);
    }

    public void setValueFree(int row, int column, Object value) {
        columns.get(column).set(row, value);
    }

    public int getRowCount() {
        return rowCount;
    }


    public void deleteColumn(int col) {
        columns.remove(col);
    }

    @Override
    public void close() throws IOException {
        //ignore
    }

}
