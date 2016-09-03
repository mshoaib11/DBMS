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

package dbs_project.query.functional;

import dbs_project.index.IndexType;
import dbs_project.query.predicate.ExpressionElement;
import dbs_project.query.statement.*;
import dbs_project.storage.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Statements {

    public static CreateColumnStatement buildCreateColumnStatement(final String tableName, final String columnName,
                                                                   final Type columnType) {
        return new CreateColumnStatement() {

            @Override
            public String getTableName() {
                return tableName;
            }

            @Override
            public String getColumnName() {
                return columnName;
            }

            @Override
            public Type getColumnType() {
                return columnType;
            }
        };
    }

    public static CreateIndexStatement buildCreateIndexStatement(final String tableName, final String columnName,
                                                                 final String indexName, final IndexType indexType) {
        return new CreateIndexStatement() {

            @Override
            public String getTableName() {
                return tableName;
            }

            @Override
            public String getColumnName() {
                return columnName;
            }

            @Override
            public String getIndexName() {
                return indexName;
            }

            @Override
            public IndexType getIndexType() {
                return indexType;
            }
        };
    }

    public static CreateTableStatement buildCreateTableStatement(final String tableName, final List<String> columnNames, final List<Type> columnTypes) {
        return new CreateTableStatement() {

            @Override
            public List<String> getColumnNames() {
                return columnNames;
            }

            @Override
            public String getTableName() {
                return tableName;
            }

            @Override
            public List<Type> getColumnTypes() {
                return columnTypes;
            }
        };
    }

    public static DeleteRowsStatement buildCreateRowStatement(final String tableName, final ExpressionElement predicate) {
        return new DeleteRowsStatement() {

            @Override
            public ExpressionElement getPredicate() {
                return predicate;
            }

            @Override
            public String getTableName() {
                return tableName;
            }
        };
    }

    public static DropColumnStatement buildDropColumnStatement(final String tableName, final String columnName) {
        return new DropColumnStatement() {

            @Override
            public String getTableName() {
                return tableName;
            }

            @Override
            public String getColumnName() {
                return columnName;
            }
        };
    }

    public static DropIndexStatement buildDropIndexStatement(final String tableName, final String columnName, final String indexName) {
        return new DropIndexStatement() {

            @Override
            public String getTableName() {
                return tableName;
            }

            @Override
            public String getColumnName() {
                return columnName;
            }

            @Override
            public String getIndexName() {
                return indexName;
            }
        };
    }

    public static DropTableStatement buildDropTableStatement(final String tableName) {
        return new DropTableStatement() {

            @Override
            public String getTableName() {
                return tableName;
            }
        };
    }

    public static InsertRowsStatement buildInsertRowsStatement(final String tableName, final List<String> columnNames,
                                                               final Iterator<List<String>> dataForRows) {
        return new InsertRowsStatement() {

            @Override
            public List<String> getColumnNames() {
                return columnNames;
            }

            @Override
            public String getTableName() {
                return tableName;
            }

            @Override
            public Iterator<List<String>> getDataForRows() {
                return dataForRows;
            }
        };
    }

    public static QueryStatement buildQueryStatement(final List<String> tableNames, final List<String> columnNames, final ExpressionElement predicate) {
        return new QueryStatement() {

            @Override
            public ExpressionElement getPredicate() {
                return predicate;
            }

            @Override
            public List<String> getColumnNames() {
                return columnNames;
            }

            @Override
            public List<String> getTableNames() {
                return tableNames;
            }
        };
    }

    public static QueryStatement buildQueryStatement(final List<String> columnNames, final ExpressionElement predicate, String... tableName) {
        return buildQueryStatement(Arrays.asList(tableName), columnNames, predicate);
    }

    public static RenameColumnStatement buildRenameColumnStatement(final String tableName, final String columnName, final String newColumnName) {
        return new RenameColumnStatement() {

            @Override
            public String getTableName() {
                return tableName;
            }

            @Override
            public String getColumnName() {
                return columnName;
            }

            @Override
            public String getNewColumnName() {
                return newColumnName;
            }
        };
    }

    public static RenameTableStatement buildRenameTableStatement(final String tableName, final String newTableName) {
        return new RenameTableStatement() {

            @Override
            public String getTableName() {
                return tableName;
            }

            @Override
            public String getNewTableName() {
                return newTableName;
            }
        };
    }

    public static DeleteRowsStatement buildDeleteRowsStatement(final String tableName, final ExpressionElement predicate) {
        return new DeleteRowsStatement() {

            @Override
            public ExpressionElement getPredicate() {
                return predicate;
            }

            @Override
            public String getTableName() {
                return tableName;
            }
        };
    }

    public static UpdateRowsStatement buildUpdateRowStatement(final String tableName, final List<String> columnNames,
                                                              final ExpressionElement predicate, final List<String> updateRowData) {
        return new UpdateRowsStatement() {

            @Override
            public List<String> getColumnNames() {
                return columnNames;
            }

            @Override
            public ExpressionElement getPredicate() {
                return predicate;
            }

            @Override
            public String getTableName() {
                return tableName;
            }

            @Override
            public List<String> getUpdateRowData() {
                return updateRowData;
            }
        };
    }

    public static String toSQL(CreateTableStatement stmnt) {
        StringBuilder sql = new StringBuilder("CREATE TABLE \"" + stmnt.getTableName() + "\" (");
        for (int i = 0; i < stmnt.getColumnNames().size(); i++) {
            Type type = stmnt.getColumnTypes().get(i);
            String t = "";
            if (type == Type.INTEGER) {
                t = "INTEGER";
            } else if (type == Type.DOUBLE) {
                t = "DOUBLE PRECISION";
            } else if (type == Type.STRING) {
                t = "VARCHAR(200)"; // for testing we use only 100 characters
            } else if (type == Type.DATE) {
                t = "DATE";
            } else if (type == Type.BOOLEAN) {
                t = "BOOLEAN";
            }
            sql.append(stmnt.getColumnNames().get(i)).append(" ").append(t).append(", ");
        }
        sql.delete(sql.length() - 2, sql.length());
        sql.append(")");
        return sql.toString();
    }

    public static String toSQL(CreateColumnStatement stmnt) {
        return "ALTER TABLE \"" + stmnt.getTableName() + "\" ADD " + stmnt.getColumnName() + " " + stmnt.getColumnType();
    }

    public static String toSQL(DropColumnStatement stmnt) {
        return "ALTER TABLE \"" + stmnt.getTableName() + "\" DROP COLUMN " + stmnt.getColumnName();
    }

    public static String toSQL(RenameColumnStatement stmnt) {
        return "RENAME COLUMN \"" + stmnt.getTableName() + "\"." + stmnt.getColumnName() + " TO " + stmnt.getNewColumnName();
    }

    public static String toSQL(RenameTableStatement stmnt) {
        return "RENAME TABLE \"" + stmnt.getTableName() + "\" TO \"" + stmnt.getNewTableName() + "\"";
    }

    public static String toSQL(QueryStatement stmnt) {
        return "SELECT";
    }

    public static List<String> toSQL(InsertRowsStatement stmnt) {
        final List<String> result = new ArrayList<String>();
        String columnNames = stmnt.getColumnNames().toString();
        columnNames = columnNames.substring(1, columnNames.length() - 1);
        Iterator<List<String>> rowIterator = stmnt.getDataForRows();
        while (rowIterator.hasNext()) {
            String rowString = rowIterator.next().toString();
            rowString = rowString.substring(1, rowString.length() - 1);
            String rowStmnt = "INSERT INTO " + stmnt.getTableName() + " (" + columnNames + ") VALUES (" + rowString + ");";
            result.add(rowStmnt);
        }
        return result;
    }
}
