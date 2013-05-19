/*

Copyright (c) 2013, Jirvan Pty Ltd
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright notice,
      this list of conditions and the following disclaimer in the documentation
      and/or other materials provided with the distribution.
    * Neither the name of Jirvan Pty Ltd nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

package com.jirvan.jidbc;

import com.jirvan.dates.*;
import com.jirvan.jidbc.internal.*;
import com.jirvan.jidbc.lang.*;
import com.jirvan.lang.*;
import com.jirvan.util.*;

import javax.sql.*;
import java.io.*;
import java.math.*;
import java.sql.*;
import java.util.*;


public class JidbcConnection {

    private Connection jdbcConnection;
    private boolean usingExternalConnection;
    private List<Results> openResultses = new ArrayList<Results>();

    private JidbcConnection(Connection jdbcConnection, boolean usingExternalConnection) {
        try {
            this.jdbcConnection = jdbcConnection;
            this.usingExternalConnection = usingExternalConnection;
            if ((!usingExternalConnection) && jdbcConnection.getAutoCommit()) {
                jdbcConnection.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public static JidbcConnection using(Connection connection) {
        return new JidbcConnection(connection, true);
    }

    public static JidbcConnection from(DataSource dataSource) {
        try {
            return new JidbcConnection(dataSource.getConnection(), false);
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public static JidbcConnection from(JdbcConnectionConfig connectionConfig) {
        return new JidbcConnection(Jdbc.getConnection(connectionConfig), false);
    }

    public static JidbcConnection fromHomeDirectoryConfigFile(String homeDirectoryConfigFile, String connectionName) {
        return new JidbcConnection(Jdbc.getConnectionFromHomeDirectoryConfigFile(homeDirectoryConfigFile, connectionName), false);
    }

    public RuntimeException rollbackCloseAndWrap(Throwable t) {
        rollbackAndClose();
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof SQLException) {
            throw new SQLRuntimeException((SQLException) t);
        } else {
            throw new RuntimeException(t);
        }
    }

    private void rollbackAndClose() {
        try {
            boolean autoCommit = jdbcConnection.getAutoCommit();
            closeAnyOpenQueryIterables();
            jdbcConnection.rollback();
            jdbcConnection.close();
            if (autoCommit) throw new RuntimeException("Expected autoCommit to be off");
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public void commitAndClose() {
        try {
            boolean autoCommit = jdbcConnection.getAutoCommit();
            closeAnyOpenQueryIterables();
            jdbcConnection.commit();
            jdbcConnection.close();
            if (autoCommit) throw new RuntimeException("Expected autoCommit to be off");
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public void closeAnyOpenQueryIterables() {
        for (Results openResults : openResultses.toArray(new Results[openResultses.size()])) {
            openResults.close();
        }
    }

    public Connection getJdbcConnection() {
        return jdbcConnection;
    }


//============================== "CRUD" (create, retrieve, update, delete) methods ==============================

    public void insert(Object row) {
        InsertHandler.insert(jdbcConnection, row, null);
    }

    public <T> T get(Class rowClass, Object pkValue) {
        try {
            return QueryForHandler.queryFor(jdbcConnection, true, rowClass, null, new Object[]{pkValue}, true);
        } catch (NotFoundRuntimeException e) {
            throw new NotFoundRuntimeException(String.format("%s:%s not found", rowClass.getSimpleName().replaceFirst("sRow$", ""), pkValue.toString()));
        }
    }

    public <T> T getIfExists(Class rowClass, Object pkValue) {
        return QueryForHandler.queryFor(jdbcConnection, false, rowClass, null, new Object[]{pkValue}, true);
    }

    public void update(Object row) {
        UpdateHandler.update(jdbcConnection, row);
    }

    public void delete(Object row) {
        DeleteHandler.delete(jdbcConnection, row);
    }


//============================== Single returned object row/value methods ==============================

    public <T> T queryFor(Class rowClass, String sql, Object... parameterValues) {
        return QueryForHandler.queryFor(jdbcConnection, true, rowClass, sql, parameterValues, false);
    }

    public <T> T queryForOptional(Class rowClass, String sql, Object... parameterValues) {
        return QueryForHandler.queryFor(jdbcConnection, false, rowClass, sql, parameterValues, false);
    }

    public String queryFor_String(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_String(jdbcConnection, true, sql, parameterValues);
    }

    public String queryForOptional_String(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_String(jdbcConnection, false, sql, parameterValues);
    }

    public Long queryFor_Long(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_Long(jdbcConnection, true, sql, parameterValues);
    }

    public Long queryForOptional_Long(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_Long(jdbcConnection, false, sql, parameterValues);
    }

    public BigDecimal queryFor_BigDecimal(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_BigDecimal(jdbcConnection, true, sql, parameterValues);
    }

    public BigDecimal queryForOptional_BigDecimal(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_BigDecimal(jdbcConnection, false, sql, parameterValues);
    }

    public Day queryFor_Day(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_Day(jdbcConnection, true, sql, parameterValues);
    }

    public Day queryForOptional_Day(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_Day(jdbcConnection, false, sql, parameterValues);
    }


//============================== Multiple returned row/object methods ==============================

    /**
     * This method executes a query against the database and returns a Results iterable that
     * can be used to process the results of the query.  Note that using this in "for each"
     * loops is OK as the JidbcConnection itself "remembers" all open Results iterables and
     * closes them when it is released if they not been closed in the normal course of events.
     * Normally the completion of the for loop that is looping through the iterable will close
     * and release the query resources.  However if there is an exception thrown during loop
     * processing the Results iterables will be closed by the JidbcConnection when it is
     * released. The JidbcConnection itself should always be released in a finally block;
     *
     * @param rowClass        The class of the rows to be returned
     * @param sql             The sql for selecting the rows from the database.  If
     *                        the sql starts with "where " then "select * from tableName "
     *                        will be prepended to the sql (the table name is determined from
     *                        the row class)
     * @param parameterValues Any parameter values associated with the sql
     * @return A Results iterable that can be used to process the results
     *         of the query.
     */
    public <T> Results<? extends T> query(Class rowClass, String sql, Object... parameterValues) {
        return new Results<T>(jdbcConnection, openResultses, rowClass, sql, parameterValues);
    }

    /**
     * This method executes a query against the database and returns a List containing the
     * results.
     *
     * @param rowClass        The class of the rows to be returned
     * @param sql             The sql for selecting the rows from the database.  If
     *                        the sql starts with "where " then "select * from tableName "
     *                        will be prepended to the sql (the table name is determined from
     *                        the row class)
     * @param parameterValues Any parameter values associated with the sql
     * @return A List containing the results
     *         of the query.
     */
    public <T> List<T> queryForList(Class rowClass, String sql, Object... parameterValues) {
        List<T> list = new ArrayList<T>();
        for (T row : this.<T>query(rowClass, sql, parameterValues)) {
            list.add(row);
        }
        return list;
    }


//============================== Pass through methods to jdbc methods ==============================

    public int executeUpdate(String sql, Object... parameters) {
        return UpdateStatementExecutor.executeUpdate(jdbcConnection, sql, parameters);
    }

//============================== "Extensions to pass through methods to jdbc methods ==============================

    /**
     * This method really is just an extension to executeUpdate that ensures that exactly
     * one row is updated.
     */
    public void updateOneRow(String sql, Object... parameters) {
        int count = UpdateStatementExecutor.executeUpdate(jdbcConnection, sql, parameters);
        if (count < 0) {
            throw new NotFoundRuntimeException("Weird error - update returned a negative count of rows updated");
        } else if (count == 0) {
            throw new NotFoundRuntimeException("No rows were found to update");
        } else if (count > 1) {
            throw new MultipleRowsRuntimeException("More than one row was updated");
        }
    }

//============================== Database metadata methods ==============================

    public String getDatabaseProductName() {
        try {
            return jdbcConnection.getMetaData().getDatabaseProductName();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public String getDatabaseProductVersion() {
        try {
            return jdbcConnection.getMetaData().getDatabaseProductVersion();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public int getDatabaseMajorVersion() {
        try {
            return jdbcConnection.getMetaData().getDatabaseMajorVersion();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public int getDatabaseMinorVersion() {
        try {
            return jdbcConnection.getMetaData().getDatabaseMinorVersion();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

//============================== Data import/export methods ==============================

    public int importTableDataFromJsonFile(Class rowClass, File inputJsonFile) {
        return JidbcImporter.importTableDataFromJsonFile(this, rowClass, inputJsonFile);
    }

    public int importTableDataFromJsonString(Class rowClass, String jsonString) {
        return JidbcImporter.importTableDataFromJsonString(this, rowClass, jsonString);
    }

    public int exportTableDataToJsonFile(String tableName, Class rowClass, File outputJsonFile) {
        return JidbcExporter.exportTableDataToJsonFile(this, rowClass, outputJsonFile);
    }

    public int exportTableDataToJsonFile(String tableName, Class rowClass, File outputJsonFile, boolean overwriteExistingFile) {
        return JidbcExporter.exportTableDataToJsonFile(this, rowClass, outputJsonFile, overwriteExistingFile);
    }

//============================== Other methods ==============================

    public Long takeSequenceNextVal(String sequenceName) {
        return SequenceHandler.takeSequenceNextVal(jdbcConnection, sequenceName);
    }


//    public static void main(String[] args) {
//
//        JidbcConnection jibc = JidbcConnection.fromHomeDirectoryConfigFile(".kfund.config", "main");
//        try {
//
//            for (Object row : jibc.<Object>query(InvoicesRow.class, "where outstanding_amount > 2000\n" +
//                                                           "  and days_overdue > 90\n" +
//                                                           "limit 3")) {
//                int sdf = 3;
//            }
//
//
//        } finally {
//            jibc.close();
//        }
//
//    }


}