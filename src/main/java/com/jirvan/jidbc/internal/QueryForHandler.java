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

package com.jirvan.jidbc.internal;

import com.jirvan.dates.*;
import com.jirvan.jidbc.lang.*;
import com.jirvan.lang.*;

import java.sql.*;
import java.util.*;

public class QueryForHandler {

    public static <T> T queryFor(Connection connection, boolean exceptionIfNotFound, Class rowClass, String sql, Object... parameterValues) {
        TableDef tableDef;
        RowExtractor rowExtractor;
        String sqlToUse;
        if (Map.class.isAssignableFrom(rowClass)) {
            tableDef = null;
            rowExtractor = new MapRowExtractor();
            sqlToUse = sql;
        } else if (Object[].class.isAssignableFrom(rowClass)) {
            tableDef = null;
            rowExtractor = new ArrayRowExtractor();
            sqlToUse = sql;
        } else {
            tableDef = TableDef.getForRowClass(rowClass);
            rowExtractor = new ObjectRowExtractor();
            sqlToUse = sql.matches("(?si)\\s*where\\s+.*")
                       ? String.format("select * from %s %s", tableDef.tableName, sql)
                       : sql;
        }
        try {
            PreparedStatement statement = connection.prepareStatement(sqlToUse);
            try {

                // Set parameter values
                for (int i = 0; i < parameterValues.length; i++) {
                    statement.setObject(i + 1, parameterValues[i]);
                }

                // Get result and check for anything other than exactly one row
                ResultSet resultSet = statement.executeQuery();
                T result;
                if (resultSet.next()) {
                    result = (T) rowExtractor.extractRowFromResultSet(rowClass, tableDef, resultSet);
                } else {
                    if (exceptionIfNotFound) {
                        throw new NoRowsRuntimeException();
                    } else {
                        result = null;
                    }
                }
                if (resultSet.next()) {
                    throw new MultipleRowsRuntimeException();
                }
                return result;

            } finally {
                statement.close();
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public static Long queryFor_Long(Connection connection, boolean exceptionIfNotFound, String sql, Object... parameterValues) {
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            try {

                // Set parameter values and execute query
                for (int i = 0; i < parameterValues.length; i++) {
                    statement.setObject(i + 1, parameterValues[i]);
                }
                ResultSet resultSet = statement.executeQuery();
                Long result;
                try {

                    // Get result and check for anything other than exactly one row
                    if (resultSet.next()) {
                        long value = resultSet.getLong(1);
                        result = resultSet.wasNull() ? null : value;
                    } else {
                        if (exceptionIfNotFound) {
                            throw new NoRowsRuntimeException();
                        } else {
                            return null;
                        }
                    }
                    if (resultSet.next()) {
                        throw new MultipleRowsRuntimeException();
                    }

                } finally {
                    resultSet.close();
                }

                // Return the result
                return result;

            } finally {
                statement.close();
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public static String queryFor_String(Connection connection, boolean exceptionIfNotFound, String sql, Object... parameterValues) {
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            try {

                // Set parameter values and execute query
                for (int i = 0; i < parameterValues.length; i++) {
                    statement.setObject(i + 1, parameterValues[i]);
                }
                ResultSet resultSet = statement.executeQuery();
                String result;
                try {

                    // Get result and check for anything other than exactly one row
                    if (resultSet.next()) {
                        String value = resultSet.getString(1);
                        result = resultSet.wasNull() ? null : value;
                    } else {
                        if (exceptionIfNotFound) {
                            throw new NoRowsRuntimeException();
                        } else {
                            return null;
                        }
                    }
                    if (resultSet.next()) {
                        throw new MultipleRowsRuntimeException();
                    }

                } finally {
                    resultSet.close();
                }

                // Return the result
                return result;

            } finally {
                statement.close();
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public static Day queryFor_Day(Connection connection, boolean exceptionIfNotFound, String sql, Object... parameterValues) {
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            try {

                // Set parameter values and execute query
                for (int i = 0; i < parameterValues.length; i++) {
                    statement.setObject(i + 1, parameterValues[i]);
                }
                ResultSet resultSet = statement.executeQuery();
                Day result;
                try {

                    // Get result and check for anything other than exactly one row
                    if (resultSet.next()) {
                        Timestamp value = resultSet.getTimestamp(1);
                        result = resultSet.wasNull() ? null : Day.from(value);
                    } else {
                        if (exceptionIfNotFound) {
                            throw new NoRowsRuntimeException();
                        } else {
                            return null;
                        }
                    }
                    if (resultSet.next()) {
                        throw new MultipleRowsRuntimeException();
                    }

                } finally {
                    resultSet.close();
                }

                // Return the result
                return result;

            } finally {
                statement.close();
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

}
