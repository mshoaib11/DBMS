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

import dbs_project.database.Database;
import dbs_project.exceptions.NoSuchColumnException;
import dbs_project.exceptions.NoSuchTableException;
import dbs_project.exceptions.TableAlreadyExistsException;
import dbs_project.query.statement.InsertRowsStatement;
import dbs_project.storage.*;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Utilities for our tests.
 */
public class Utils {

    private static final String SYMBOLS = "!$%&?=+*#-.,:;@|[]()0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    //
    public static final int NULL_FACTOR = 10;
    public static final int MAX_RANDOM_STRING_SIZE = 30;
    public static final long SEED = 12345L;
    public static final Random RANDOM = new Random(SEED);
    public static final ByteBuffer HELPER_BUFFER = ByteBuffer.allocate(8);
    //
    
    // old streams
    public static PrintStream oldOut = null;
    public static PrintStream oldErr = null;

    private Utils() {
        throw new AssertionError("fail.");
    }
    
    public static PrintStream getOut() {
    	return oldOut;
    }
    
    public static PrintStream getErr() {
    	return oldErr;
    }
    
    public static void redirectStreams() {
    	oldOut = System.out;
    	/*System.setOut(new PrintStream(new OutputStream() {
			@Override
			public void write(int arg0) throws IOException {}
		}));*/
		
    	oldErr = System.err;
		/*System.setErr(new PrintStream(new OutputStream() {
			@Override
			public void write(int arg0) throws IOException {}
		}));*/
		
		//System.setSecurityManager(new DBMSecurityManager());
    }
    
    public static void revertStreams() {
    	System.setOut(oldOut);
    	System.setErr(oldErr);
    }

    public static long randomLong() {
        RANDOM.nextBytes(HELPER_BUFFER.array());
        return HELPER_BUFFER.getLong(0);
    }

    public static Object generatePossibleRandom(Type type) {
        if (type.getJavaClass().isPrimitive() || RANDOM.nextInt() % NULL_FACTOR != 0) {
            switch (type) {
                case STRING:
                    return generateRandomString(RANDOM.nextInt(MAX_RANDOM_STRING_SIZE));
                case INTEGER:
                    return RANDOM.nextInt();
                case DOUBLE:
                    return generateRandomDouble();
                case DATE:
                    return new Date(RANDOM.nextLong());
                case BOOLEAN:
                    return RANDOM.nextBoolean();
                default:
                    break;
            }
        }
        return null;
    }

    public static String generateRandomString(int len) {
        final StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append(SYMBOLS.charAt(RANDOM.nextInt(SYMBOLS.length())));
        }
        return sb.toString();
    }

    public static double generateRandomDouble() {
        double d;
        do {
            d = Double.longBitsToDouble(randomLong());
        } while(Double.isNaN(d) || Double.isInfinite(d));
        return d;
    }

    public static boolean areObjectsEqual(Object one, Object two) {
        return one == two || (one != null && one.equals(two));
    }

    public static boolean areObjectsNotEqual(Object one, Object two) {
        return !areObjectsEqual(one, two);
    }

    public static int compareObjects(Comparable left, Comparable right) {
        if (left != null) {
            if (right != null) {
                //left != null && right != null -> let compare decide
                return left.compareTo(right);
            } else {
                //left != null && right == null -> 1 as left > right
                return 1;
            }
        } else {
            if (right == null) {
                //left == right == null
                return 0;
            } else {
                //left == null && right != null -> -1 as left < right
                return -1;
            }
        }
    }

    public static Column getColumnByName(Table table, String columnName) throws NoSuchColumnException {
        final Map<String, ColumnMetaData> schema = table.getTableMetaData().getTableSchema();
        final ColumnMetaData colMetaData = schema.get(columnName);
        return table.getColumn(colMetaData.getId());
    }

    public static Table getTableByName(String tableName, StorageLayer storage) throws NoSuchTableException {
        return storage.getTable(
                storage.getDatabaseSchema().get(tableName).getId());
    }

    public static ArrayIntList convertIdIteratorToList(IdCursor iter) {
        final ArrayIntList result = new ArrayIntList();
        while (iter.next()) {
            result.add(iter.getId());
        }
        return result;
    }

    public static String resultSetToHtmlTable(java.sql.ResultSet rs) throws SQLException {
        int rowCount = 0;
        final StringBuilder result = new StringBuilder();
        result.append("<P ALIGN='center'>\n<TABLE BORDER=1>\n");
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        //header
        result.append("\t<TR>\n");
        for (int i = 0; i < columnCount; ++i) {
            result.append("\t\t<TH>").append(rsmd.getColumnLabel(i + 1)).append("</TH>\n");
        }
        result.append("\t</TR>\n");
        //data
        while (rs.next()) {
            ++rowCount;
            result.append("\t<TR>\n");
            for (int i = 0; i < columnCount; ++i) {
                String value = rs.getString(i + 1);
                if (rs.wasNull()) {
                    value = "&#060;null&#062;";
                }
                result.append("\t\t<TD>").append(value).append("</TD>\n");
            }
            result.append("\t</TR>\n");
        }
        result.append("</TABLE>\n</P>\n");
        return result.toString();
    }

    public static String rowCursorToHtmlTable(RowCursor rc, boolean withTableNames) {
        int rowCount = 0;
        final StringBuilder result = new StringBuilder();
        result.append("<P ALIGN='center'>\n<TABLE BORDER=1>\n");
        //header
        result.append("\t<TR>\n");
        if (rc.next()) {
            final RowMetaData rsmd = rc.getMetaData();
            int columnCount = rsmd.getColumnCount();
            for (int i = 0; i < columnCount; ++i) {
                ColumnMetaData cmd = rsmd.getColumnMetaData(i);
                String columnName = withTableNames ? cmd.getLabel() : cmd.getName();
                result.append("\t\t<TH>").append(columnName).append("</TH>\n");
            }
            result.append("\t</TR>\n");
            //data
            do {
                rowCount++;
                result.append("\t<TR>\n");
                for (int i = 0; i < columnCount; ++i) {
                    String value = rc.getString(i);
                    if (value == null) {
                        value = "&#060;null&#062;";
                    }
                    result.append("\t\t<TD>").append(value).append("</TD>\n");
                }
                result.append("\t</TR>\n");
            } while (rc.next());
        }
        result.append("</TABLE>\n</P>\n");
        return result.toString();
    }

    public static Table createEmptyTableForRowSchema(String tableName, RowMetaData rowMetaData, StorageLayer storage) throws TableAlreadyExistsException, NoSuchTableException {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(rowMetaData);
        Preconditions.checkNotNull(storage);
        final Map<String, Type> schema = new HashMap<String, Type>();
        for (int i = 0; i < rowMetaData.getColumnCount(); ++i) {
            ColumnMetaData colMeta = rowMetaData.getColumnMetaData(i);
            schema.put(colMeta.getName(), colMeta.getType());
        }
        int tableId = storage.createTable(tableName, schema);
        return storage.getTable(tableId);
    }

    public static Table createEmptyTableForSimpleColumns(String tableName, List<SimpleColumn> columnDescr, StorageLayer storage) throws TableAlreadyExistsException, NoSuchTableException {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(columnDescr);
        final Map<String, Type> schema = new HashMap<String, Type>();
        for (int i = 0; i < columnDescr.size(); ++i) {
            SimpleColumn curColDesc = columnDescr.get(i);
            schema.put(curColDesc.getName(), curColDesc.getType());
        }
        int tableId = storage.createTable(tableName, schema);
        return storage.getTable(tableId);
    }

    public static boolean compareRowsByGetObject(Row reference, Row toCompare, int[] mapping) {
        for (int i = 0; i < mapping.length; ++i) {
            final Object testValue = toCompare.getObject(i);
            final Object referenceValue = reference.getObject(mapping[i]);
            if (Utils.areObjectsNotEqual(referenceValue, testValue)) {
                return false;
            }

        }
        return true;
    }

    public static boolean compareRowsByPrimitives(final Row reference, final Row toCompare, final int[] mapping, final Type[] types) {
        for (int i = 0; i < mapping.length; ++i) {
            final boolean equalValues;
            final int mappedIndex = mapping[i];
            switch (types[i]) {
                case STRING:
                    final String testValue = toCompare.getString(i);
                    final String referenceValue = reference.getString(mappedIndex);
                    equalValues = Utils.areObjectsEqual(testValue, referenceValue);
                    break;
                case INTEGER:
                    final int refInt = reference.getInteger(mappedIndex);
                    equalValues = refInt == toCompare.getInteger(i);
                    break;
                case DOUBLE:
                    final double refDbl = reference.getDouble(mappedIndex);
                    equalValues = refDbl == toCompare.getDouble(i);
                    break;
                case DATE:
                    final Date testValue2 = toCompare.getDate(i);
                    final Date referenceValue2 = reference.getDate(mappedIndex);
                    equalValues = Utils.areObjectsEqual(testValue2, referenceValue2);
                    break;
                case BOOLEAN:
                    final boolean refBool = reference.getBoolean(mappedIndex);
                    equalValues = refBool == toCompare.getBoolean(i);
                    break;
                default:
                    final Object testValue3 = toCompare.getObject(i);
                    final Object referenceValue3 = reference.getObject(mappedIndex);
                    equalValues = Utils.areObjectsEqual(testValue3, referenceValue3);
                    break;
            }
//            if (!equalValues || reference.isNull(mappedIndex) != toCompare.isNull(i)) {
            if (!equalValues) {
                return false;
            }
        }
        return true;
    }

    public static boolean compareColumns(Column reference, Column toCompare) throws Exception {
        Preconditions.checkNotNull(reference);
        Preconditions.checkNotNull(toCompare);
        final int rowCount = reference.getMetaData().getRowCount();
        switch (reference.getMetaData().getType()) {
            case STRING:
                for (int i = 0; i < rowCount; ++i) {
                    if (Utils.areObjectsNotEqual(reference.getString(i), toCompare.getString(i))) {
                        return false;
                    }
                }
                break;
            case INTEGER:
                for (int i = 0; i < rowCount; ++i) {
                    final int ref = reference.getInteger(i);
                    if (ref != toCompare.getInteger(i) || (ref == Type.NULL_VALUE_INTEGER && reference.isNull(i) != toCompare.isNull(i))) {
                        return false;
                    }
                }
                break;
            case DOUBLE:
                for (int i = 0; i < rowCount; ++i) {
                    final double ref = reference.getDouble(i);
                    if (ref != toCompare.getDouble(i) || (ref == Type.NULL_VALUE_DOUBLE && reference.isNull(i) != toCompare.isNull(i))) {
                        return false;
                    }
                }
                break;
            case DATE:
                for (int i = 0; i < rowCount; ++i) {
                    if (Utils.areObjectsNotEqual(reference.getDate(i), toCompare.getDate(i))) {
                        return false;
                    }
                }
                break;
            case BOOLEAN:
                for (int i = 0; i < rowCount; ++i) {
                    final boolean ref = reference.getBoolean(i);
                    if (ref != toCompare.getBoolean(i) || (ref == Type.NULL_VALUE_BOOLEAN && reference.isNull(i) != toCompare.isNull(i))) {
                        return false;
                    }
                }
                break;
            default:
                for (int i = 0; i < rowCount; ++i) {
                    if (Utils.areObjectsNotEqual(reference.getObject(i), toCompare.getObject(i))) {
                        return false;
                    }
                }
                break;
        }
        return true;
    }

    public static boolean compareColumnCursors(ColumnCursor reference, ColumnCursor toCompare) throws Exception {
        while (reference.next()) {
            if (toCompare.next()) {
                if (!compareColumns(reference, toCompare)) {
                    return false;
                }
            } else {
                return false;
            }
        }
        if (toCompare.next()) {
            return false;
        }
        return true;
    }

    public static boolean compareRowCursors(RowCursor reference, RowCursor toCompare) throws Exception {
        Preconditions.checkNotNull(reference, "Reference result is null!");
        Preconditions.checkNotNull(toCompare, "Result to check is null!");
        if (toCompare.next()) {
            if (reference.next()) {
                boolean refHasNext = true;
                final MappingInfo mappingInfo = getFieldMappingFromMetaData(reference.getMetaData(), toCompare.getMetaData(), false);
                if (mappingInfo != null) {
                    final int[] mapping = mappingInfo.getColumnMapping();
                    final Type[] types = mappingInfo.getColumnTypes();
                    int position = 0;
                    do {
                        if (refHasNext) {
                            ++position;
                            if (!compareRowsByPrimitives(reference, toCompare, mapping, types)) {
//                if (!compareRowsByGetObject(reference, toCompare, mapping)) {
//                        LOG.info("Results have different values: row: " + position + ", column: " + i;
                                return false;
                            }

                        } else {
//                LOG.info("Result have different sizes. Expected only " + position + " rows!");
                            return false;
                        }
                        refHasNext = reference.next();
                    } while (toCompare.next());
                    return !refHasNext;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return !reference.next();
        }
    }

    private static MappingInfo getFieldMappingFromMetaData(final RowMetaData referenceMetaData, final RowMetaData toCompareMetaData, final boolean considerTableNames) throws Exception {
        Preconditions.checkNotNull(referenceMetaData);
        Preconditions.checkNotNull(toCompareMetaData);
        final int refColumnCount = referenceMetaData.getColumnCount();
        final int toTestColCount = toCompareMetaData.getColumnCount();
        if (refColumnCount == toTestColCount) {
            final Map<ColumnInfo, Integer> toComparemapping = new HashMap<ColumnInfo, Integer>();
            final int[] columnMapping = new int[refColumnCount];
            final Type[] types = new Type[refColumnCount];
            for (int i = 0; i < refColumnCount; ++i) {
                final ColumnMetaData columnMetaData = toCompareMetaData.getColumnMetaData(i);
                final String toTestColumnName = columnMetaData.getName();
                final Table srcTable = columnMetaData.getSourceTable();
                final String toTestTableName;
                if (srcTable != null && considerTableNames) {
                    toTestTableName = columnMetaData.getSourceTable().getTableMetaData().getName();
                } else {
                    toTestTableName = "";
                }
                final Type toTestType = columnMetaData.getType();
                types[i] = toTestType;
                final ColumnInfo colInf = new ColumnInfo(toTestColumnName, toTestTableName, toTestType);
                toComparemapping.put(colInf, i);
            }
            for (int i = 0; i < refColumnCount; ++i) {
                final ColumnMetaData columnMetaData = referenceMetaData.getColumnMetaData(i);
                final String refColumnName = columnMetaData.getName();
                final String refTableName;
                final Table srcTable = columnMetaData.getSourceTable();
                if (srcTable != null && considerTableNames) {
                    refTableName = columnMetaData.getSourceTable().getTableMetaData().getName();
                } else {
                    refTableName = "";
                }
                final Type refType = columnMetaData.getType();
                final ColumnInfo colInf = new ColumnInfo(refColumnName, refTableName, refType);
                final Integer positionInToCompare = toComparemapping.get(colInf);
                Preconditions.checkArgument(positionInToCompare != null, "ResultSets have different Schemas! No entry found for: " + colInf + ". Existing columns: " + toComparemapping);
                columnMapping[positionInToCompare] = i;
            }
            return new MappingInfo(columnMapping, types);
        } else {
            return null;
        }
    }

    public static Type getTypeForJavaResultSetType(final int javaResultSetType) throws Exception {
        switch (javaResultSetType) {
            case java.sql.Types.VARCHAR:
                return Type.STRING;
            case java.sql.Types.INTEGER:
                return Type.INTEGER;
            case java.sql.Types.DOUBLE:
                return Type.DOUBLE;
            case java.sql.Types.DATE:
                return Type.DATE;
            case java.sql.Types.BOOLEAN:
                return Type.BOOLEAN;
            default:
                throw new Exception("Unsupported Type: java.sql.Type(" + javaResultSetType + ")");
        }
    }

    static final class MappingInfo {

        private final int[] columnMapping;
        private final Type[] columnTypes;

        public MappingInfo(int[] columnMapping, Type[] types) {
            this.columnMapping = columnMapping;
            this.columnTypes = types;
        }

        public int[] getColumnMapping() {
            return columnMapping;
        }

        public Type[] getColumnTypes() {
            return columnTypes;
        }
    }

    static final class ColumnInfo {

        public ColumnInfo(String columnName, String tableName, Type columnType) {
            this.columnName = columnName;
            this.tableName = tableName;
            this.columnType = columnType;
        }
        final String columnName;
        final String tableName;
        final Type columnType;

        @Override
        public boolean equals(Object obj) {
            //generated
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ColumnInfo other = (ColumnInfo) obj;
            if ((this.columnName == null) ? (other.columnName != null) : !this.columnName.equals(other.columnName)) {
                return false;
            }
            if ((this.tableName == null) ? (other.tableName != null) : !this.tableName.equals(other.tableName)) {
                return false;
            }
            if (this.columnType != other.columnType) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            //generated
            int hash = 3;
            hash = 67 * hash + (this.columnName != null ? this.columnName.hashCode() : 0);
            hash = 67 * hash + (this.tableName != null ? this.tableName.hashCode() : 0);
            hash = 67 * hash + (this.columnType != null ? this.columnType.hashCode() : 0);
            return hash;
        }

        @Override
        public String toString() {
            return tableName + "." + columnName + "(" + columnType + ")";
        }
    }

    public static InsertRowsStatement getInsertStatementFromFile(InputStream is, final String tableName) throws IOException {
        String lineString;
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is), 8192);
        try {
            if ((lineString = bufferedReader.readLine()) != null) {
                final List<List<String>> rowData = new ArrayList<List<String>>();
                final String[] columnDescString = lineString.split(TableInputFileReader.COLUMN_SEPARATOR);
                final List<String> names = new ArrayList<String>(columnDescString.length);
                for (int i = 0; i < columnDescString.length; ++i) {
                    final String columnDescriptor = columnDescString[i];
                    final String[] nameToType = columnDescriptor.split(TableInputFileReader.NAME_TYPE_SEPARATOR);
                    Preconditions.checkArgument(nameToType.length == 2);
                    names.add(i, nameToType[0]);
                }
                while ((lineString = bufferedReader.readLine()) != null) {
                    final String[] lineValues = lineString.split(TableInputFileReader.COLUMN_SEPARATOR);
                    for (int i = 0; i < lineValues.length; i++) {
                        String value = lineValues[i];
                        if (value.startsWith(" ")) {
                            lineValues[i] = value.substring(1);
                        }
                    }
                    Preconditions.checkArgument(lineValues.length >= names.size(), "row has wrong column count!");
                    rowData.add(Arrays.asList(lineValues));
                }

                final InsertRowsStatement result = new InsertRowsStatement() {
                    @Override
                    public Iterator<List<String>> getDataForRows() {
                        return rowData.iterator();
                    }

                    @Override
                    public String getTableName() {
                        return tableName;
                    }

                    @Override
                    public List<String> getColumnNames() {
                        return names;
                    }
                };
                return result;
            } else {
                throw new RuntimeException("Empty file!");
            }
        } finally {
            IOUtils.closeQuietly(bufferedReader);
        }
    }

    public static InsertRowsStatement getInsertStatement(String tableName, List<SimpleColumn> columns, int rowCount) {
        List<String> columnNames = new ArrayList<>();
        List<List<String>> data = new ArrayList<>();
        java.sql.Date tmp = new java.sql.Date(0);
        
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));

        for (SimpleColumn col : columns) {
            columnNames.add(col.getName());
        }
        for (int i = 0; i < rowCount; ++i) {
            List<String> rowData = new ArrayList<>();

            for (SimpleColumn col : columns) {
                if (col.getType() == Type.DATE) {
                    /*tmp.setTime(col.getDate(i).getTime());
                    rowData.add(tmp.toString());*/
                	rowData.add(format.format(col.getDate(i)));
                } else {
                    rowData.add(col.getString(i));
                }
            }
            data.add(rowData);
        }
        return new InsertRowsStatementImpl(data, tableName, columnNames);
    }

    private static class InsertRowsStatementImpl implements InsertRowsStatement {

        private List<List<String>> data;
        private String tableName;
        private List<String> columnNames;

        public InsertRowsStatementImpl(List<List<String>> data, String tableName, List<String> columnNames) {
            this.data = data;
            this.tableName = tableName;
            this.columnNames = columnNames;
        }

        @Override
        public Iterator<List<String>> getDataForRows() {
            return data.iterator();
        }

        @Override
        public String getTableName() {
            return tableName;
        }

        @Override
        public List<String> getColumnNames() {
            return columnNames;
        }

    }
}
