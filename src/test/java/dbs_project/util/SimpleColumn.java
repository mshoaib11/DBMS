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

import dbs_project.storage.Column;
import dbs_project.storage.ColumnMetaData;
import dbs_project.storage.Table;
import dbs_project.storage.Type;
import org.apache.commons.collections.primitives.ArrayIntList;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Implementation that we use for our tests. Supports basic manipulation
 * and randomization.
 * <p/>
 * This column implementation is very simple and not tuned for performance in
 * any way (e.g. using objects instead of primitives).
 * <p/>
 * You should *not* consider this a guide for your own implementation!
 */
public final class SimpleColumn implements Column, ColumnMetaData {

    private final List data;
    private final int id;
    private final String name;
    private final Type type;
    //
    private ArrayIntList rowIdMapping;
    private Table srcTable;

    public SimpleColumn(int id, String name, Type type) {
        this(new ArrayList(), id, name, type);
    }

    public SimpleColumn(List data, int id, String name, Type type) {
        this(data, id, name, type, null, null);
    }

    public SimpleColumn(int size, int id, String name, Type type) {
        this(new ArrayList(size), id, name, type, null, null);
        randomizeCompleteColumn(size);
    }

    public SimpleColumn(List data, int id, String name, Type type, ArrayIntList rowIdMapping, Table srcTable) {
        this.data = data;
        this.id = id;
        this.name = name;
        this.rowIdMapping = rowIdMapping;
        this.type = type;
        this.srcTable = srcTable;
    }

    @Override
    public ColumnMetaData getMetaData() {
        return this;
    }

    @Override
    public int getInteger(int index) throws IndexOutOfBoundsException, ClassCastException {
        if (isNull(index)) {
            return Type.NULL_VALUE_INTEGER;
        } else {
            Object value = data.get(index);
            if (value instanceof Integer) {
                return (Integer) value;
            } else if (value instanceof Double) {
                return ((Double) value).intValue();
            } else {
                throw new ClassCastException(String.valueOf(value));
            }
        }
    }

    @Override
    public boolean getBoolean(int index) throws IndexOutOfBoundsException, ClassCastException {
        return isNull(index) ? Type.NULL_VALUE_BOOLEAN : (Boolean) data.get(index);
    }

    @Override
    public double getDouble(int index) throws IndexOutOfBoundsException, ClassCastException {
        if (isNull(index)) {
            return Type.NULL_VALUE_DOUBLE;
        } else {
            Object value = data.get(index);
            if (value instanceof Double) {
                return (Double) value;
            } else if (value instanceof Integer) {
                return (Integer) value;
            } else {
                throw new ClassCastException(String.valueOf(value));
            }
        }
    }

    @Override
    public Date getDate(int index) throws IndexOutOfBoundsException, ClassCastException {
        return (Date) data.get(index);
    }

    @Override
    public String getString(int index) throws IndexOutOfBoundsException {
        return isNull(index) ? null : String.valueOf(data.get(index));
    }

    @Override
    public Object getObject(int index) throws IndexOutOfBoundsException {
        return data.get(index);
    }

    @Override
    public boolean isNull(int index) throws IndexOutOfBoundsException {
        return data.get(index) == null;
    }

    @Override
    public int getRowCount() {
        return data.size();
    }

    @Override
    public Table getSourceTable() {
        return srcTable;
    }

    @Override
    public String getLabel() {
        return srcTable == null ? name : srcTable.getTableMetaData().getName() + "." + name;
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public int getRowId(int positionInColumn) throws IndexOutOfBoundsException {
        return rowIdMapping.get(positionInColumn);
    }

    @Override
    public int getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    public void remove(int index) {
        data.remove(index);
    }

    public void set(int index, Object value) {
        data.set(index, value);
    }

    public void setRandom(int index) {
        data.set(index, Utils.generatePossibleRandom(type));
    }

    public final void randomizeCompleteColumn() {
        for (int i = 0; i < data.size(); ++i) {
            data.set(i, Utils.generatePossibleRandom(type));
        }
    }

    public final void randomizeCompleteColumn(int newSize) {
        data.clear();
        for (int i = 0; i < newSize; ++i) {
            data.add(Utils.generatePossibleRandom(type));
        }
    }

    public void setRowIdMapping(ArrayIntList rowIdMapping) {
        this.rowIdMapping = rowIdMapping;
    }

    public void setSrcTable(Table srcTable) {
        this.srcTable = srcTable;
    }
}
