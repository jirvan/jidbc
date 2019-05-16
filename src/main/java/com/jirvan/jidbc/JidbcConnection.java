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

import com.jirvan.dates.Day;
import com.jirvan.jidbc.internal.DeleteHandler;
import com.jirvan.jidbc.internal.InsertHandler;
import com.jirvan.jidbc.internal.JidbcExporter;
import com.jirvan.jidbc.internal.JidbcImporter;
import com.jirvan.jidbc.internal.QueryForHandler;
import com.jirvan.jidbc.internal.Results;
import com.jirvan.jidbc.internal.SaveHandler;
import com.jirvan.jidbc.internal.SequenceHandler;
import com.jirvan.jidbc.internal.UpdateHandler;
import com.jirvan.jidbc.internal.UpdateStatementExecutor;
import com.jirvan.jidbc.lang.MultipleRowsRuntimeException;
import com.jirvan.lang.NotFoundRuntimeException;
import com.jirvan.lang.SQLRuntimeException;
import com.jirvan.util.DatabaseType;
import com.jirvan.util.Jdbc;
import com.jirvan.util.JdbcConnectionConfig;

import javax.sql.DataSource;
import java.io.File;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static com.jirvan.util.Assertions.*;

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
            return (RuntimeException) t;
        } else if (t instanceof SQLException) {
            return new SQLRuntimeException((SQLException) t);
        } else {
            return new RuntimeException(t);
        }
    }

    public void rollbackAndClose() {
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

    public <T> T insert(T row) {
        InsertHandler.insert(jdbcConnection, row, null);
        return row;
    }

    public <T> T get(Class rowClass, Object pkValue) {
        assertNotNull(rowClass, "Supplied row class is null");
        assertNotNull(pkValue, "Supplied primary key value is null");
        try {
            return QueryForHandler.queryFor(jdbcConnection, true, rowClass, null, new Object[]{pkValue}, true, false, false);
        } catch (NotFoundRuntimeException e) {
            throw new NotFoundRuntimeException(String.format("%s:%s not found", rowClass.getSimpleName().replaceFirst("sRow$", ""), pkValue.toString()));
        }
    }

    public <T> T getForUpdate(Class rowClass, Object pkValue) {
        assertNotNull(rowClass, "Supplied row class is null");
        assertNotNull(pkValue, "Supplied primary key value is null");
        try {
            return QueryForHandler.queryFor(jdbcConnection, true, rowClass, null, new Object[]{pkValue}, true, false, true);
        } catch (NotFoundRuntimeException e) {
            throw new NotFoundRuntimeException(String.format("%s:%s not found", rowClass.getSimpleName().replaceFirst("sRow$", ""), pkValue.toString()));
        }
    }

    public <T> T getIfExists(Class rowClass, Object pkValue) {
        assertNotNull(rowClass, "Supplied row class is null");
        assertNotNull(pkValue, "Supplied primary key value is null");
        return QueryForHandler.queryFor(jdbcConnection, false, rowClass, null, new Object[]{pkValue}, true, false, false);
    }

    public <T> T getIfExistsForUpdate(Class rowClass, Object pkValue) {
        assertNotNull(rowClass, "Supplied row class is null");
        assertNotNull(pkValue, "Supplied primary key value is null");
        return QueryForHandler.queryFor(jdbcConnection, false, rowClass, null, new Object[]{pkValue}, true, false, true);
    }

    public void update(Object row) {
        UpdateHandler.update(jdbcConnection, row);
    }

    public void save(Object row) {
        SaveHandler.save(jdbcConnection, row);
    }

    public void delete(Object row) {
        DeleteHandler.delete(jdbcConnection, row);
    }


//============================== Single returned object row/value methods ==============================

    public <T> T queryFor(Class rowClass, String sql, Object... parameterValues) {
        return QueryForHandler.queryFor(jdbcConnection, true, rowClass, sql, parameterValues, false, false, false);
    }

    public <T> T queryForForUpdate(Class rowClass, String sql, Object... parameterValues) {
        return QueryForHandler.queryFor(jdbcConnection, true, rowClass, sql, parameterValues, false, false, true);
    }

    public <T> T queryForAndIgnoreMissingResultSetColumns(Class rowClass, String sql, Object... parameterValues) {
        return QueryForHandler.queryFor(jdbcConnection, true, rowClass, sql, parameterValues, false, true, false);
    }

    public <T> T queryForForUpdateAndIgnoreMissingResultSetColumns(Class rowClass, String sql, Object... parameterValues) {
        return QueryForHandler.queryFor(jdbcConnection, true, rowClass, sql, parameterValues, false, true, true);
    }

    public <T> T queryForOptional(Class rowClass, String sql, Object... parameterValues) {
        return QueryForHandler.queryFor(jdbcConnection, false, rowClass, sql, parameterValues, false, false, false);
    }

    public <T> T queryForForUpdateOptional(Class rowClass, String sql, Object... parameterValues) {
        return QueryForHandler.queryFor(jdbcConnection, false, rowClass, sql, parameterValues, false, false, true);
    }

    public <T> T queryForOptionalIgnoringMissingResultSetColumns(Class rowClass, String sql, Object... parameterValues) {
        return QueryForHandler.queryFor(jdbcConnection, false, rowClass, sql, parameterValues, false, true, false);
    }

    public <T> T queryForForUpdateOptionalIgnoringMissingResultSetColumns(Class rowClass, String sql, Object... parameterValues) {
        return QueryForHandler.queryFor(jdbcConnection, false, rowClass, sql, parameterValues, false, true, true);
    }

    public String queryFor_String(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_String(jdbcConnection, true, sql, parameterValues);
    }

    public String queryForOptional_String(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_String(jdbcConnection, false, sql, parameterValues);
    }

    public Integer queryFor_Integer(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_Integer(jdbcConnection, true, sql, parameterValues);
    }

    public Long queryFor_Long(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_Long(jdbcConnection, true, sql, parameterValues);
    }

    public Long queryForOptional_Long(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_Long(jdbcConnection, false, sql, parameterValues);
    }

    public Boolean queryFor_Boolean(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_Boolean(jdbcConnection, true, sql, parameterValues);
    }

    public Boolean queryForOptional_Boolean(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_Boolean(jdbcConnection, false, sql, parameterValues);
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

    public LocalDate queryFor_LocalDate(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_LocalDate(jdbcConnection, true, sql, parameterValues);
    }

    public LocalDateTime queryFor_LocalDateTime(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_LocalDateTime(jdbcConnection, true, sql, parameterValues);
    }

    public Day queryForOptional_Day(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_Day(jdbcConnection, false, sql, parameterValues);
    }

    public LocalDate queryForOptional_LocalDate(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_LocalDate(jdbcConnection, false, sql, parameterValues);
    }

    public LocalDateTime queryForOptional_LocalDateTime(String sql, Object... parameterValues) {
        return QueryForHandler.queryFor_LocalDateTime(jdbcConnection, false, sql, parameterValues);
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
     * of the query.
     * @see #queryIgnoringMissingResultSetColumns(Class, String, Object...) query
     */
    public <T> Results<? extends T> query(Class rowClass, String sql, Object... parameterValues) {
        return new Results<T>(jdbcConnection, openResultses, rowClass, sql, false, false, parameterValues);
    }

    /**
     * This method executes a query against the database locking selected rows (it adds
     * a "for update clause to the sql) and returns a Results iterable that
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
     * of the query.
     * @see #queryIgnoringMissingResultSetColumns(Class, String, Object...) query
     */
    public <T> Results<? extends T> queryForUpdate(Class rowClass, String sql, Object... parameterValues) {
        return new Results<T>(jdbcConnection, openResultses, rowClass, sql, false, true, parameterValues);
    }

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
     * @param rowClass        The class of the rows to be returned.  Note that for this method
     *                        any fields in class that do not have a corresponding column in
     *                        the sql result set will be set to null (unlike the {@link #query(Class, String, Object...) query} method
     *                        which will throw a runtime exception)
     * @param sql             The sql for selecting the rows from the database.  If
     *                        the sql starts with "where " then "select * from tableName "
     *                        will be prepended to the sql (the table name is determined from
     *                        the row class)
     * @param parameterValues Any parameter values associated with the sql
     * @return A Results iterable that can be used to process the results
     * of the query.
     * @see #query(Class, String, Object...) query
     */
    public <T> Results<? extends T> queryIgnoringMissingResultSetColumns(Class rowClass, String sql, Object... parameterValues) {
        return new Results<T>(jdbcConnection, openResultses, rowClass, sql, true, false, parameterValues);
    }

    /**
     * This method executes a query against the database locking selected rows (it adds
     * a "for update clause to the sql) and returns a Results iterable that
     * can be used to process the results of the query.  Note that using this in "for each"
     * loops is OK as the JidbcConnection itself "remembers" all open Results iterables and
     * closes them when it is released if they not been closed in the normal course of events.
     * Normally the completion of the for loop that is looping through the iterable will close
     * and release the query resources.  However if there is an exception thrown during loop
     * processing the Results iterables will be closed by the JidbcConnection when it is
     * released. The JidbcConnection itself should always be released in a finally block;
     *
     * @param rowClass        The class of the rows to be returned.  Note that for this method
     *                        any fields in class that do not have a corresponding column in
     *                        the sql result set will be set to null (unlike the {@link #query(Class, String, Object...) query} method
     *                        which will throw a runtime exception)
     * @param sql             The sql for selecting the rows from the database.  If
     *                        the sql starts with "where " then "select * from tableName "
     *                        will be prepended to the sql (the table name is determined from
     *                        the row class)
     * @param parameterValues Any parameter values associated with the sql
     * @return A Results iterable that can be used to process the results
     * of the query.
     * @see #query(Class, String, Object...) query
     */
    public <T> Results<? extends T> queryForUpdateIgnoringMissingResultSetColumns(Class rowClass, String sql, Object... parameterValues) {
        return new Results<T>(jdbcConnection, openResultses, rowClass, sql, true, true, parameterValues);
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
     * of the query.
     */
    public <T> List<T> queryForList(Class rowClass, String sql, Object... parameterValues) {
        List<T> list = new ArrayList<T>();
        for (T row : this.<T>query(rowClass, sql, parameterValues)) {
            list.add(row);
        }
        return list;
    }

    /**
     * This method executes a query against the database locking selected rows (it adds
     * a "for update clause to the sql)  and returns a List containing the
     * results.
     *
     * @param rowClass        The class of the rows to be returned
     * @param sql             The sql for selecting the rows from the database.  If
     *                        the sql starts with "where " then "select * from tableName "
     *                        will be prepended to the sql (the table name is determined from
     *                        the row class)
     * @param parameterValues Any parameter values associated with the sql
     * @return A List containing the results
     * of the query.
     */
    public <T> List<T> queryForUpdateForList(Class rowClass, String sql, Object... parameterValues) {
        List<T> list = new ArrayList<T>();
        for (T row : this.<T>queryForUpdate(rowClass, sql, parameterValues)) {
            list.add(row);
        }
        return list;
    }

    /**
     * This method executes a query against the database and returns a List containing the
     * results.
     *
     * @param rowClass        The class of the rows to be returned.  Note that for this method
     *                        any fields in class that do not have a corresponding column in
     *                        the sql result set will be set to null (unlike the {@link #queryForList(Class, String, Object...) queryForList} method
     *                        which will throw a runtime exception)
     * @param sql             The sql for selecting the rows from the database.  If
     *                        the sql starts with "where " then "select * from tableName "
     *                        will be prepended to the sql (the table name is determined from
     *                        the row class)
     * @param parameterValues Any parameter values associated with the sql
     * @return A List containing the results
     * of the query.
     * @see #queryForList(Class, String, Object...) queryForList
     */
    public <T> List<T> queryForListIgnoringMissingResultSetColumns(Class rowClass, String sql, Object... parameterValues) {
        List<T> list = new ArrayList<T>();
        for (T row : this.<T>queryIgnoringMissingResultSetColumns(rowClass, sql, parameterValues)) {
            list.add(row);
        }
        return list;
    }

    /**
     * This method executes a query against the database locking selected rows (it adds
     * a "for update clause to the sql)  and returns a List containing the
     * results.
     *
     * @param rowClass        The class of the rows to be returned.  Note that for this method
     *                        any fields in class that do not have a corresponding column in
     *                        the sql result set will be set to null (unlike the {@link #queryForList(Class, String, Object...) queryForList} method
     *                        which will throw a runtime exception)
     * @param sql             The sql for selecting the rows from the database.  If
     *                        the sql starts with "where " then "select * from tableName "
     *                        will be prepended to the sql (the table name is determined from
     *                        the row class)
     * @param parameterValues Any parameter values associated with the sql
     * @return A List containing the results
     * of the query.
     * @see #queryForList(Class, String, Object...) queryForList
     */
    public <T> List<T> queryForUpdateForListIgnoringMissingResultSetColumns(Class rowClass, String sql, Object... parameterValues) {
        List<T> list = new ArrayList<T>();
        for (T row : this.<T>queryForUpdateIgnoringMissingResultSetColumns(rowClass, sql, parameterValues)) {
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

    public DatabaseType getDatabaseType() {
        return DatabaseType.get(getDatabaseProductName());
    }

    public DatabaseType getDatabaseTypeIfSupported(DatabaseType... supportedDatabaseTypes) {
        return DatabaseType.getIfSupported(getDatabaseProductName(), supportedDatabaseTypes);
    }

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

    public boolean tableExists(String tableName) {
        try {
            DatabaseMetaData databaseMetaData = getJdbcConnection().getMetaData();
            ResultSet tables = databaseMetaData.getTables(null, null, tableName, null);
            while (tables.next()) {
                if (tableName.equals(tables.getString("table_name"))) {
                    return true;
                }
            }
            return false;
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


//============================== Database specific methods ==============================

    public Long lastSQLiteAutoId() {
        return queryFor_Long("select last_insert_rowid()");
    }

}