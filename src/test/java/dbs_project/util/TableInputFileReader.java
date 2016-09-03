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
import dbs_project.storage.Type;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Reads table information from a file.
 */
public final class TableInputFileReader {

    //
    public static final String NULL_STRING = "<NULL>";
    public static final String COLUMN_SEPARATOR = "\\|";
    public static final String NAME_TYPE_SEPARATOR = "::";

    //
    private TableInputFileReader() {
        throw new AssertionError("fail.");
    }

    public static List<SimpleColumn> createSimpleColumnsFromFile(InputStream is, boolean schemaOnly) throws IOException {
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is), 8192);
        String lineString;
        if ((lineString = bufferedReader.readLine()) != null) {
            final String[] columnDescString = lineString.split(COLUMN_SEPARATOR);
            final String[] names = new String[columnDescString.length];
            final Type[] types = new Type[columnDescString.length];
            final List[] columnDatas = new List[columnDescString.length];

            for (int i = 0; i < columnDescString.length; ++i) {
                final String columnDescriptor = columnDescString[i];
                final String[] nameToType = columnDescriptor.split(NAME_TYPE_SEPARATOR);
                Preconditions.checkArgument(nameToType.length == 2);
                names[i] = nameToType[0];
                final String type = nameToType[1];
                for (final Type curType : Type.values()) {
                    if (type.equals(curType.toString())) {
                        types[i] = curType;
                        //create typesafe instances of array list according to type
                        switch (curType) {
                            case STRING:
                                columnDatas[i] = new ArrayList<String>();
                                break;
                            case INTEGER:
                                columnDatas[i] = new ArrayList<Integer>();
                                break;
                            case DOUBLE:
                                columnDatas[i] = new ArrayList<Double>();
                                break;
                            case DATE:
                                columnDatas[i] = new ArrayList<Date>();
                                break;
                            case BOOLEAN:
                                columnDatas[i] = new ArrayList<Boolean>();
                                break;
                            default:
                                throw new RuntimeException("Unsupported type");
                        }
                    }
                }
            }
            if (!schemaOnly) {
                while ((lineString = bufferedReader.readLine()) != null) {
                    final String[] lineValues = lineString.split(COLUMN_SEPARATOR);
                    Preconditions.checkArgument(lineValues.length >= columnDatas.length, "row has wrong column count!");
                    for (int col = 0; col < columnDatas.length; ++col) {
                        final List currentColumn = columnDatas[col];
                        final String value = lineValues[col];
                        if (NULL_STRING.equals(value)) {
                            currentColumn.add(null);
                        } else {
                            switch (types[col]) {
                                case STRING:
                                    currentColumn.add(value);
                                    break;
                                case DATE:
                                    currentColumn.add(new Date(java.sql.Date.valueOf(value).getTime()));
                                    break;
                                case INTEGER:
                                    currentColumn.add(Integer.parseInt(value));
                                    break;
                                case DOUBLE:
                                    currentColumn.add(Double.parseDouble(value));
                                    break;
                                case BOOLEAN:
                                    currentColumn.add(Boolean.parseBoolean(value));
                                    break;
                                default:
                                    throw new RuntimeException("Unsupported type");
                            }
                        }
                    }
                }
            }
            IOUtils.closeQuietly(bufferedReader);
            final List<SimpleColumn> result = new ArrayList<SimpleColumn>(columnDatas.length);
            for (int col = 0; col < columnDatas.length; ++col) {
                result.add(new SimpleColumn(columnDatas[col], col, names[col], types[col]));

            }
            return result;
        } else {
            throw new RuntimeException("Could not create table schema and column data from given file!");
        }
    }
}
