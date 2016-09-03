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

import dbs_project.util.SimpleColumn;
import dbs_project.util.Utils;

import java.sql.*;
import java.util.List;

public class DerbyDB {

    private final String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    private final String dbName = "testDB";
    private final String connectionURL = "jdbc:derby:memory:" + dbName + ";create=true";
    private Connection conn = null;
    private Statement s = null;

    public DerbyDB() {
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(connectionURL);
            s = conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            DriverManager.getConnection("jdbc:derby:memory:" + dbName + ";drop=true");
        } catch (SQLException e) {
            //expected
        }
        try {
            DriverManager.getConnection("jdbc:derby:memory:" + dbName + ";shutdown=true");
        } catch (SQLException e) {
            //expected
        }
    }

    public void shutDown() {
        try {
            DriverManager.getConnection("jdbc:derby:memory:" + dbName + ";shutdown=true");
        } catch (SQLException e) {
            //expected
        }
    }

    public void executeSQL(String sql) {
        try {
            Utils.getOut().println("Executing SQL statement: " + sql);
            s.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public ResultSet executeQuery(String sql) {
        try {
            Utils.getOut().println("Executing SQL statement: " + sql);
            return s.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void createTable(String tableName, String schema) {
        String sql = "CREATE TABLE \"" + tableName + "\" (" + schema + ")";
        try {
            Utils.getOut().println("Executing SQL statement: " + sql);
            s.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public int update(String tableName, String setStatement, String whereStatement) {
        String sql;
        if ("".equals(whereStatement)) {
            sql = "UPDATE \"" + tableName + "\" SET " + setStatement;
        } else {
            sql = "UPDATE \"" + tableName + "\" SET " + setStatement + " WHERE " + whereStatement;
        }
        try {
            Utils.getOut().println("Executing SQL statement: " + sql);
            return s.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public int delete(String tableName, String predicate) {
        String sql = "DELETE FROM \"" + tableName + "\" WHERE " + predicate;
        try {
            Utils.getOut().println("Executing SQL statement: " + sql);
            return s.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public void importData(String tableName, String fileName) {
        try {
            s = conn.createStatement();
            String createString = "CALL SYSCS_UTIL.SYSCS_IMPORT_TABLE (NULL, '" + tableName + "' , '"
                    + fileName + "' , '|', NULL, NULL, 0)";
            s.execute(createString);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void insertData(String tableName, List<SimpleColumn> columns, int rowCount) {
        try {
            String insert = "INSERT INTO " + tableName + " (";
            insert += columns.get(0).getName();
            for (int i = 1; i < columns.size(); ++i) {
                insert += ", " + columns.get(i).getName();
            }
            insert += ") VALUES (";
            insert += "?";
            for (int i = 1; i < columns.size(); ++i) {
                insert += ", ?";
            }
            insert += ")";
            PreparedStatement stmt = conn.prepareStatement(insert);
            Date date = new Date(0);

            for (int i = 0; i < rowCount; ++i) {
                for (int j = 0; j < columns.size(); ++j) {
                    SimpleColumn col = columns.get(j);

                    switch (col.getType()) {
                        case INTEGER:
                            stmt.setInt(j + 1, col.getInteger(i));
                            break;
                        case DOUBLE:
                            stmt.setDouble(j + 1, col.getDouble(i));
                            break;
                        case STRING:
                            stmt.setString(j + 1, col.getString(i));
                            break;
                        case DATE:
                            date.setTime(col.getDate(i).getTime());
                            stmt.setDate(j + 1, date);
                            break;
                        case BOOLEAN:
                            stmt.setBoolean(j + 1, col.getBoolean(i));
                            break;
                    }
                }
                stmt.addBatch();
            }
            conn.setAutoCommit(false);
            stmt.executeBatch();
            conn.commit();
            conn.setAutoCommit(true);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void beginTransaction() {
        Utils.getOut().println("Derby: Starting transaction");
        try {
            conn.setAutoCommit(false);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void commit() {
        Utils.getOut().println("Derby: Commiting transaction");
        try {
            conn.setAutoCommit(true);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void abort() {
        Utils.getOut().println("Derby: Commiting transaction");
        try {
            conn.rollback();
            conn.setAutoCommit(true);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

}
