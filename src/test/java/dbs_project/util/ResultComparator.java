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

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 */
public final class ResultComparator {

    private ResultComparator() {
        throw new AssertionError("fail");
    }

    public static boolean compareResultsSimple(java.sql.ResultSet jdbcReferenceResult, dbs_project.storage.RowCursor cursorToTest) throws Exception {
        Preconditions.checkNotNull(jdbcReferenceResult, "Reference result is null!");
        Preconditions.checkNotNull(cursorToTest, "Result to check is null!");
        MappingInfo mappingInfo = null;
        int hashChecksumReference = 0;
        int hashChecksumTest = 0;
        if (cursorToTest.next()) {
            mappingInfo = getFieldMappingFromMetaData(jdbcReferenceResult, cursorToTest);
            do {
                StringBuilder appender = new StringBuilder();
                for (int i = 0; i < cursorToTest.getMetaData().getColumnCount(); ++i) {
                    String value = cursorToTest.getString(i);
                    //temporary derby importer problem fix
                    if (value != null) {
                        value = value.trim();
                    }
                    appender.append(value);
                }
                hashChecksumTest += appender.toString().hashCode();
            } while (cursorToTest.next());
        }
        while (jdbcReferenceResult.next()) {
            StringBuilder appender = new StringBuilder();
            for (int i : mappingInfo.columnMapping) {
                String value = jdbcReferenceResult.getString(i);
                //temporary derby importer problem fix
                if (value != null) {
                    value = value.trim();
                }
                appender.append(value);
            }
            hashChecksumReference += appender.toString().hashCode();
        }
        jdbcReferenceResult.close();
        cursorToTest.close();
        return hashChecksumReference == hashChecksumTest;
    }

    private static MappingInfo getFieldMappingFromMetaData(final java.sql.ResultSet javaResult, final dbs_project.storage.RowCursor dbsResult) throws Exception {
        java.sql.ResultSetMetaData javaMetaData = javaResult.getMetaData();
        dbs_project.storage.RowMetaData dbsMetaData = dbsResult.getMetaData();
        Preconditions.checkNotNull(javaMetaData);
        Preconditions.checkNotNull(dbsMetaData);
        final int javaColCount = javaMetaData.getColumnCount();
        final int dbsColCount = dbsMetaData.getColumnCount();
        final Map<ColumnInfo, Integer> dbsMapping = new HashMap<ColumnInfo, Integer>();
        final int[] columnMapping = new int[dbsColCount];
        final Type[] typeMapping = new Type[dbsColCount];
        Preconditions.checkArgument(javaColCount == dbsColCount, "ResultSets have different column count! Expected: " + javaColCount + ", Found: " + dbsColCount);
        for (int i = 0; i < dbsColCount; ++i) {
            final String dbsColumnName = dbsMetaData.getColumnMetaData(i).getName().toLowerCase();
            final String dbsTableName = dbsMetaData.getColumnMetaData(i).getLabel().split(Pattern.quote("."), 2)[0].toLowerCase(); //getSourceTable().getTableMetaData().getName().toLowerCase();
            final Type dbsType = dbsMetaData.getColumnMetaData(i).getType();
            final ColumnInfo dbsColumnInfo = new ColumnInfo(dbsColumnName, dbsTableName, dbsType);
            typeMapping[i] = dbsType;
            dbsMapping.put(dbsColumnInfo, i);
        }
        //+1 because java.sql.ResultSet starts counting columns form 1, not 0
        for (int i = 1; i <= javaColCount; ++i) {
            final String javaColumnName = javaMetaData.getColumnName(i).toLowerCase();
            final String javaTableName = javaMetaData.getTableName(i).toLowerCase();
            final Type javaType = getTypeForJavaResultSetType(javaMetaData.getColumnType(i));
            final ColumnInfo javaColumnInfo = new ColumnInfo(javaColumnName, javaTableName, javaType);
            final Integer positionInDbs = dbsMapping.get(javaColumnInfo);
            Preconditions.checkArgument(positionInDbs != null, "ResultSets have different Schemas! No entry found for: " + javaColumnInfo + ". Existing columns: " + dbsMapping);
            columnMapping[positionInDbs] = i;

        }
        return new MappingInfo(typeMapping, columnMapping);
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

    static class MappingInfo {

        public MappingInfo(Type[] typeMapping, int[] columnMMapping) {
            this.typeMapping = typeMapping;
            this.columnMapping = columnMMapping;
        }
        final Type[] typeMapping;
        final int[] columnMapping;
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
}
