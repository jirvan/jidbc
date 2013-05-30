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

import javax.sql.*;
import java.math.*;
import java.util.*;

public class Jidbc {


//============================== "CRUD" (create, retrieve, update, delete) methods ==============================

    public static void insert(DataSource dataSource, Object row) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            jidbc.insert(row);

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static <T> T get(DataSource dataSource, Class rowClass, Object pkValue) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            T row = jidbc.get(rowClass, pkValue);

            jidbc.commitAndClose();
            return row;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static <T> T getIfExists(DataSource dataSource, Class rowClass, Object pkValue) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            T ifExists = jidbc.getIfExists(rowClass, pkValue);

            jidbc.commitAndClose();
            return ifExists;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static void update(DataSource dataSource, Object row) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            jidbc.update(row);

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static void delete(DataSource dataSource, Object row) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            jidbc.delete(row);

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }


//============================== Single returned object row/value methods ==============================

    public static <T> T queryFor(DataSource dataSource, Class rowClass, String sql, Object... parameterValues) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            T row = jidbc.queryFor(rowClass, sql, parameterValues);

            jidbc.commitAndClose();
            return row;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static <T> T queryForOptional(DataSource dataSource, Class rowClass, String sql, Object... parameterValues) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            T row = jidbc.queryForOptional(rowClass, sql, parameterValues);

            jidbc.commitAndClose();
            return row;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static String queryFor_String(DataSource dataSource, String sql, Object... parameterValues) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            String value = jidbc.queryFor_String(sql, parameterValues);

            jidbc.commitAndClose();
            return value;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static String queryForOptional_String(DataSource dataSource, String sql, Object... parameterValues) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            String value = jidbc.queryForOptional_String(sql, parameterValues);

            jidbc.commitAndClose();
            return value;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static Integer queryFor_Integer(DataSource dataSource, String sql, Object... parameterValues) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            Integer value = jidbc.queryFor_Integer(sql, parameterValues);

            jidbc.commitAndClose();
            return value;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static Long queryFor_Long(DataSource dataSource, String sql, Object... parameterValues) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            Long value = jidbc.queryFor_Long(sql, parameterValues);

            jidbc.commitAndClose();
            return value;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static Long queryForOptional_Long(DataSource dataSource, String sql, Object... parameterValues) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            Long value = jidbc.queryForOptional_Long(sql, parameterValues);

            jidbc.commitAndClose();
            return value;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static BigDecimal queryFor_BigDecimal(DataSource dataSource, String sql, Object... parameterValues) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            BigDecimal value = jidbc.queryFor_BigDecimal(sql, parameterValues);

            jidbc.commitAndClose();
            return value;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static BigDecimal queryForOptional_BigDecimal(DataSource dataSource, String sql, Object... parameterValues) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            BigDecimal value = jidbc.queryForOptional_BigDecimal(sql, parameterValues);

            jidbc.commitAndClose();
            return value;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static Day queryFor_Day(DataSource dataSource, String sql, Object... parameterValues) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            Day value = jidbc.queryFor_Day(sql, parameterValues);

            jidbc.commitAndClose();
            return value;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static Day queryForOptional_Day(DataSource dataSource, String sql, Object... parameterValues) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            Day value = jidbc.queryForOptional_Day(sql, parameterValues);

            jidbc.commitAndClose();
            return value;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

//============================== Multiple returned row/object methods ==============================


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
    public static <T> List<T> queryForList(DataSource dataSource, Class rowClass, String sql, Object... parameterValues) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            List<T> row = jidbc.queryForList(rowClass, sql, parameterValues);

            jidbc.commitAndClose();
            return row;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

//============================== Pass through methods to jdbc methods ==============================

    public static int executeUpdate(DataSource dataSource, String sql, Object... parameters) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            int count = jidbc.executeUpdate(sql, parameters);

            jidbc.commitAndClose();
            return count;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

//============================== Extensions to pass through methods to jdbc methods ==============================

    /**
     * This method really is just an extension to executeUpdate that ensures that exactly
     * one row is updated.
     */
    public static void updateOneRow(DataSource dataSource, String sql, Object... parameters) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            jidbc.updateOneRow(sql, parameters);

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

//============================== Database metadata methods ==============================

    public static String getDatabaseProductName(DataSource dataSource) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            String value = jidbc.getDatabaseProductName();

            jidbc.commitAndClose();
            return value;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static String getDatabaseProductVersion(DataSource dataSource) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            String value = jidbc.getDatabaseProductVersion();

            jidbc.commitAndClose();
            return value;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static int getDatabaseMajorVersion(DataSource dataSource) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            int value = jidbc.getDatabaseMajorVersion();

            jidbc.commitAndClose();
            return value;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    public static int getDatabaseMinorVersion(DataSource dataSource) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            int value = jidbc.getDatabaseMinorVersion();

            jidbc.commitAndClose();
            return value;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

////============================== Data import/export methods ==============================
//
//    public void importTableDataFromJsonFile(Class rowClass, File inputJsonFile) {
//        JidbcImporter.importTableDataFromJsonFile(this, rowClass, inputJsonFile);
//    }
//
//    public void importTableDataFromJsonString(Class rowClass, String jsonString) {
//        JidbcImporter.importTableDataFromJsonString(this, rowClass, jsonString);
//    }
//
//    public void exportTableDataToJsonFile(String tableName, Class rowClass, File outputJsonFile) {
//        JidbcExporter.exportTableDataToJsonFile(this, rowClass, outputJsonFile);
//    }
//
//    public void exportTableDataToJsonFile(String tableName, Class rowClass, File outputJsonFile, boolean overwriteExistingFile) {
//        JidbcExporter.exportTableDataToJsonFile(this, rowClass, outputJsonFile, overwriteExistingFile);
//    }
//
////============================== Other methods ==============================
//
//    public Long takeSequenceNextVal(String sequenceName) {
//        return SequenceHandler.takeSequenceNextVal(jdbcConnection, sequenceName);
//    }


}
