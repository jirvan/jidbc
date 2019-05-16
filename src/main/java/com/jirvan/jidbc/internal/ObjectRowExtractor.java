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

import com.jirvan.dates.Day;
import com.jirvan.dates.Hour;
import com.jirvan.dates.Millisecond;
import com.jirvan.dates.Minute;
import com.jirvan.dates.Month;
import com.jirvan.dates.Second;
import com.jirvan.lang.SQLRuntimeException;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class ObjectRowExtractor<T> implements RowExtractor<T> {

    private List<ColumnDef> applicableColumnDefs; // only used for curtailed result sets

    private boolean containsColumn(ResultSet resultSet, String columnName) {
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                if (metaData.getColumnName(i).equals(columnName)) return true;
            }
            return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public T extractRowFromResultSet(Class rowClass, final RowDef rowDef, final ResultSet resultSet, final boolean ignoreMissingResultSetColumns) {
        try {

            if (ignoreMissingResultSetColumns && applicableColumnDefs == null) {
                applicableColumnDefs = new ArrayList<>();
                for (ColumnDef columnDef : rowDef.columnDefs) {
                    if (containsColumn(resultSet, columnDef.columnName)) {
                        applicableColumnDefs.add(columnDef);
                    }
                }
            }

            // Create and return the row
            final T row;
            try {
                row = (T) rowClass.newInstance();
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            List<ColumnDef> columnDefs = applicableColumnDefs != null ? applicableColumnDefs : rowDef.columnDefs;
            for (final ColumnDef columnDef : columnDefs) {


                AttributeValueHandler.performForClass(columnDef.attributeType,
                                                      new AttributeValueHandler.ClassAction() {

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
                                                                  Boolean value = resultSet.getBoolean(columnDef.columnName);
                                                                  if (resultSet.wasNull()) {
                                                                      columnDef.setValue(row, null);
                                                                  } else {
                                                                      columnDef.setValue(row, value);
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

                                                          public void performFor_Month() {
                                                              try {
                                                                  String value = resultSet.getString(columnDef.columnName);
                                                                  if (resultSet.wasNull()) {
                                                                      columnDef.setValue(row, null);
                                                                  } else {
                                                                      columnDef.setValue(row, Month.fromString(value));
                                                                  }
                                                              } catch (SQLException e) {
                                                                  throw new SQLRuntimeException(e);
                                                              }
                                                          }

                                                          public void performFor_Day() {
                                                              try {
                                                                  if (columnDef.storeAsTimestamp) {
                                                                      Timestamp value = resultSet.getTimestamp(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, Day.from(new java.util.Date(value.getTime())));
                                                                      }
                                                                  } else {
                                                                      String value = resultSet.getString(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, Day.fromString(value));
                                                                      }
                                                                  }
                                                              } catch (SQLException e) {
                                                                  throw new SQLRuntimeException(e);
                                                              }
                                                          }

                                                          public void performFor_LocalDate() {
                                                              try {
                                                                  if (columnDef.storeAsTimestamp) {
                                                                      Timestamp value = resultSet.getTimestamp(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, value.toLocalDateTime().toLocalDate());
                                                                      }
                                                                  } else {
                                                                      String value = resultSet.getString(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, LocalDate.parse(value));
                                                                      }
                                                                  }
                                                              } catch (SQLException e) {
                                                                  throw new SQLRuntimeException(e);
                                                              }
                                                          }

                                                          public void performFor_LocalDateTime() {
                                                              try {
                                                                  if (columnDef.storeAsTimestamp) {
                                                                      Timestamp value = resultSet.getTimestamp(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, value.toLocalDateTime());
                                                                      }
                                                                  } else {
                                                                      String value = resultSet.getString(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, LocalDateTime.parse(value));
                                                                      }
                                                                  }
                                                              } catch (SQLException e) {
                                                                  throw new SQLRuntimeException(e);
                                                              }
                                                          }

                                                          public void performFor_Hour() {
                                                              try {
                                                                  if (columnDef.storeAsTimestamp) {
                                                                      Timestamp value = resultSet.getTimestamp(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, Hour.from(new java.util.Date(value.getTime())));
                                                                      }
                                                                  } else {
                                                                      String value = resultSet.getString(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, Hour.fromString(value));
                                                                      }
                                                                  }
                                                              } catch (SQLException e) {
                                                                  throw new SQLRuntimeException(e);
                                                              }
                                                          }

                                                          public void performFor_Minute() {
                                                              try {
                                                                  if (columnDef.storeAsTimestamp) {
                                                                      Timestamp value = resultSet.getTimestamp(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, Minute.from(new java.util.Date(value.getTime())));
                                                                      }
                                                                  } else {
                                                                      String value = resultSet.getString(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, Minute.fromString(value));
                                                                      }
                                                                  }
                                                              } catch (SQLException e) {
                                                                  throw new SQLRuntimeException(e);
                                                              }
                                                          }

                                                          public void performFor_Second() {
                                                              try {
                                                                  if (columnDef.storeAsTimestamp) {
                                                                      Timestamp value = resultSet.getTimestamp(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, Second.from(new java.util.Date(value.getTime())));
                                                                      }
                                                                  } else {
                                                                      String value = resultSet.getString(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, Second.fromString(value));
                                                                      }
                                                                  }
                                                              } catch (SQLException e) {
                                                                  throw new SQLRuntimeException(e);
                                                              }
                                                          }

                                                          public void performFor_Millisecond() {
                                                              try {
                                                                  if (columnDef.storeAsTimestamp) {
                                                                      Timestamp value = resultSet.getTimestamp(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, Millisecond.from(new java.util.Date(value.getTime())));
                                                                      }
                                                                  } else {
                                                                      String value = resultSet.getString(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, Millisecond.fromString(value));
                                                                      }
                                                                  }
                                                              } catch (SQLException e) {
                                                                  throw new SQLRuntimeException(e);
                                                              }
                                                          }

                                                          public void performFor_ZonedDateTime() {
                                                              try {
                                                                  if (columnDef.storeAsTimestamp) {
                                                                      Timestamp value = resultSet.getTimestamp(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, ZonedDateTime.ofInstant(value.toInstant(), ZoneId.systemDefault()));
                                                                      }
                                                                  } else {
                                                                      String value = resultSet.getString(columnDef.columnName);
                                                                      if (resultSet.wasNull()) {
                                                                          columnDef.setValue(row, null);
                                                                      } else {
                                                                          columnDef.setValue(row, ZonedDateTime.parse(value));
                                                                      }
                                                                  }
                                                              } catch (SQLException e) {
                                                                  throw new SQLRuntimeException(e);
                                                              }
                                                          }

                                                          public void performFor_Enum(Class fieldClass) {
                                                              try {
                                                                  String value = resultSet.getString(columnDef.columnName);
                                                                  if (resultSet.wasNull()) {
                                                                      columnDef.setValue(row, null);
                                                                  } else {
                                                                      columnDef.setValue(row, Enum.valueOf(fieldClass, value));
                                                                  }
                                                              } catch (SQLException e) {
                                                                  throw new SQLRuntimeException(e);
                                                              }
                                                          }

                                                          public void performFor_byteArray() {
                                                              try {
                                                                  byte[] value = resultSet.getBytes(columnDef.columnName);
                                                                  if (resultSet.wasNull()) {
                                                                      columnDef.setValue(row, null);
                                                                  } else {
                                                                      columnDef.setValue(row, value);
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
