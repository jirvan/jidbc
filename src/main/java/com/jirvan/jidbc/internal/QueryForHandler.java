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
import java.util.Date;

public class QueryForHandler {

    public static <T> T queryFor(Connection connection, boolean exceptionIfNotFound, Class rowClass, String sql, Object... parameterValues) {
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            try {

                // Set parameter values
                for (int i = 0; i < parameterValues.length; i++) {
                    statement.setObject(i + 1, parameterValues[i]);
                }

                // Get result and check for anything other than exactly one row
                ResultSet resultSet = statement.executeQuery();
                T result;
                if (resultSet.next()) {
                    result = extractObjectRowFromResultset(rowClass, resultSet);
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

    private static <T> T extractObjectRowFromResultset(Class rowClass, final ResultSet resultSet) {
        return extractObjectRowFromResultset(rowClass, TableDef.getForRowClass(rowClass), resultSet);
    }

    private static <T> T extractObjectRowFromResultset(Class rowClass, TableDef tableDef, final ResultSet resultSet) {
        try {
            // Create and return the row
            final T row = (T) rowClass.newInstance();
            for (final ColumnDef columnDef : tableDef.columnDefMap.values()) {


                FieldValueHandler.performForClass(columnDef.field.getType(),
                                                  new FieldValueHandler.ClassAction() {

                                                      public void performFor_String() {
                                                          try {
                                                              columnDef.field.set(row, resultSet.getString(columnDef.columnName));
                                                          } catch (IllegalAccessException e) {
                                                              throw new RuntimeException(e);
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          }
                                                      }

                                                      public void performFor_Integer() {
                                                          try {
                                                              int value = resultSet.getInt(columnDef.columnName);
                                                              if (resultSet.wasNull()) {
                                                                  columnDef.field.set(row, null);
                                                              } else {
                                                                  columnDef.field.set(row, value);
                                                              }
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          } catch (IllegalAccessException e) {
                                                              throw new RuntimeException(e);
                                                          }
                                                      }

                                                      public void performFor_Long() {
                                                          try {
                                                              long value = resultSet.getLong(columnDef.columnName);
                                                              if (resultSet.wasNull()) {
                                                                  columnDef.field.set(row, null);
                                                              } else {
                                                                  columnDef.field.set(row, value);
                                                              }
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          } catch (IllegalAccessException e) {
                                                              throw new RuntimeException(e);
                                                          }
                                                      }

                                                      public void performFor_Boolean() {
                                                          try {
                                                              int value = resultSet.getInt(columnDef.columnName);
                                                              if (resultSet.wasNull()) {
                                                                  columnDef.field.set(row, null);
                                                              } else {
                                                                  columnDef.field.set(row, value != 0);
                                                              }
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          } catch (IllegalAccessException e) {
                                                              throw new RuntimeException(e);
                                                          }
                                                      }

                                                      public void performFor_BigDecimal() {
                                                          try {
                                                              columnDef.field.set(row, resultSet.getBigDecimal(columnDef.columnName));
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          } catch (IllegalAccessException e) {
                                                              throw new RuntimeException(e);
                                                          }
                                                      }

                                                      public void performFor_Date() {
                                                          try {
                                                              Timestamp value = resultSet.getTimestamp(columnDef.columnName);
                                                              if (resultSet.wasNull()) {
                                                                  columnDef.field.set(row, null);
                                                              } else {
                                                                  columnDef.field.set(row, new Date(value.getTime()));
                                                              }
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          } catch (IllegalAccessException e) {
                                                              throw new RuntimeException(e);
                                                          }
                                                      }

                                                      public void performFor_Day() {
                                                          try {
                                                              Timestamp value = resultSet.getTimestamp(columnDef.columnName);
                                                              if (resultSet.wasNull()) {
                                                                  columnDef.field.set(row, null);
                                                              } else {
                                                                  columnDef.field.set(row, Day.from(new Date(value.getTime())));
                                                              }
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          } catch (IllegalAccessException e) {
                                                              throw new RuntimeException(e);
                                                          }
                                                      }

                                                  });

            }
            return row;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
