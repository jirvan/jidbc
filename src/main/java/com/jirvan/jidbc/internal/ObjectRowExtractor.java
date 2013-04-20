/*
 * Copyright (c) 2013, Jirvan Pty Ltd
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright notice,
 *       this list of conditions and the following disclaimer in the documentation
 *       and/or other materials provided with the distribution.
 *     * Neither the name of Jirvan Pty Ltd nor the names of its contributors
 *       may be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.jirvan.jidbc.internal;

import com.jirvan.dates.*;
import com.jirvan.lang.*;

import java.sql.*;

public class ObjectRowExtractor<T> implements RowExtractor<T> {

    public T extractRowFromResultSet(Class rowClass, TableDef tableDef, final ResultSet resultSet) {
        try {
            // Create and return the row
            final T row;
            try {
                row = (T) rowClass.newInstance();
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            for (final ColumnDef columnDef : tableDef.columnDefs) {


                FieldValueHandler.performForClass(columnDef.field.getType(),
                                                  new FieldValueHandler.ClassAction() {

                                                      public void performFor_String() {
                                                          try {
                                                              columnDef.setValue(row, resultSet.getString(columnDef.columnName));
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          }
                                                      }

                                                      public void performFor_Integer() {
                                                          try {
                                                              int value = resultSet.getInt(columnDef.columnName);
                                                              if (resultSet.wasNull()) {
                                                                  columnDef.setValue(row, null);
                                                              } else {
                                                                  columnDef.setValue(row, value);
                                                              }
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          }
                                                      }

                                                      public void performFor_Long() {
                                                          try {
                                                              long value = resultSet.getLong(columnDef.columnName);
                                                              if (resultSet.wasNull()) {
                                                                  columnDef.setValue(row, null);
                                                              } else {
                                                                  columnDef.setValue(row, value);
                                                              }
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          }
                                                      }

                                                      public void performFor_Boolean() {
                                                          try {
                                                              int value = resultSet.getInt(columnDef.columnName);
                                                              if (resultSet.wasNull()) {
                                                                  columnDef.setValue(row, null);
                                                              } else {
                                                                  columnDef.setValue(row, value != 0);
                                                              }
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          }
                                                      }

                                                      public void performFor_BigDecimal() {
                                                          try {
                                                              columnDef.setValue(row, resultSet.getBigDecimal(columnDef.columnName));
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          }
                                                      }

                                                      public void performFor_Date() {
                                                          try {
                                                              Timestamp value = resultSet.getTimestamp(columnDef.columnName);
                                                              if (resultSet.wasNull()) {
                                                                  columnDef.setValue(row, null);
                                                              } else {
                                                                  columnDef.setValue(row, new java.util.Date(value.getTime()));
                                                              }
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          }
                                                      }

                                                      public void performFor_Day() {
                                                          try {
                                                              Timestamp value = resultSet.getTimestamp(columnDef.columnName);
                                                              if (resultSet.wasNull()) {
                                                                  columnDef.setValue(row, null);
                                                              } else {
                                                                  columnDef.setValue(row, Day.from(new java.util.Date(value.getTime())));
                                                              }
                                                          } catch (SQLException e) {
                                                              throw new SQLRuntimeException(e);
                                                          }
                                                      }

                                                  });

            }
            return row;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

}
