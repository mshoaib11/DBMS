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
import dbs_project.storage.*;
import java.io.IOException;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;

/**
 * RowCursor implementation that just produces #rows random rows according to
 * the given schema.
 */
public final class RandomRowCursor implements RowCursor, RowMetaData {

    private final Type[] typeSchema;
    private final String[] nameSchema;
    private final Object[] currentRowData;
    //
    private int position;
    private final int rowCount;

    public RandomRowCursor(Map<String, Type> schema, int rowCount) {
        Preconditions.checkNotNull(schema);
        Preconditions.checkArgument(rowCount >= 0);
        this.rowCount = rowCount;
        this.currentRowData = new Object[schema.size()];
        this.nameSchema = new String[schema.size()];
        this.typeSchema = new Type[schema.size()];
        int i = 0;
        for (Map.Entry<String, Type> entry : schema.entrySet()) {
            this.nameSchema[i] = entry.getKey();
            this.typeSchema[i] = entry.getValue();
            ++i;
        }
        this.position = -1;
    }

    @Override
    public RowMetaData getMetaData() {
        return this;
    }

    @Override
    public int getInteger(int index) throws IndexOutOfBoundsException, ClassCastException {
        return isNull(index) ? Type.NULL_VALUE_INTEGER : (Integer) currentRowData[index];
    }

    @Override
    public boolean getBoolean(int index) throws IndexOutOfBoundsException, ClassCastException {
        return isNull(index) ? Type.NULL_VALUE_BOOLEAN : (Boolean) currentRowData[index];
    }

    @Override
    public double getDouble(int index) throws IndexOutOfBoundsException, ClassCastException {
        return isNull(index) ? Type.NULL_VALUE_DOUBLE : (Double) currentRowData[index];
    }

    @Override
    public Date getDate(int index) throws IndexOutOfBoundsException, ClassCastException {
        return (Date) currentRowData[index];
    }

    @Override
    public String getString(int index) throws IndexOutOfBoundsException {
        return isNull(index) ? null : String.valueOf(currentRowData[index]);
    }

    @Override
    public Object getObject(int index) throws IndexOutOfBoundsException {
        return currentRowData[index];
    }

    @Override
    public boolean isNull(int index) throws IndexOutOfBoundsException {
        return currentRowData[index] == null;
    }

    @Override
    public boolean next() {
        ++position;
        if (position < rowCount) {
            for (int i = 0; i < currentRowData.length; ++i) {
                currentRowData[i] = Utils.generatePossibleRandom(typeSchema[i]);
            }
            return true;
        } else if (position == rowCount) {
            Arrays.fill(currentRowData, null);

        }
        return false;
    }

    @Override
    public int getColumnCount() {
        return nameSchema.length;
    }

    @Override
    public ColumnMetaData getColumnMetaData(int positionInTheRow) throws IndexOutOfBoundsException {
        return new RandomColumnMetaDataImpl(positionInTheRow);
    }

    @Override
    public int getId() {
        return position;
    }

    @Override
    public void close() throws IOException {
        //ignore
    }

    final class RandomColumnMetaDataImpl implements ColumnMetaData {

        public RandomColumnMetaDataImpl(int index) {
            this.index = index;
        }

        private final int index;

        @Override
        public int getRowCount() {
            return rowCount;
        }

        @Override
        public Table getSourceTable() {
            return null;
        }

        @Override
        public String getLabel() {
            return getName();
        }

        @Override
        public Type getType() {
            return typeSchema[index];
        }

        @Override
        public int getRowId(int positionInColumn) throws IndexOutOfBoundsException {
            return positionInColumn;
        }

        @Override
        public int getId() {
            return index;
        }

        @Override
        public String getName() {
            return nameSchema[index];
        }
    }
}
