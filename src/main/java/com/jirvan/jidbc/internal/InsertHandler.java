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

import com.jirvan.dates.Day;
import com.jirvan.dates.Hour;
import com.jirvan.dates.Millisecond;
import com.jirvan.dates.Minute;
import com.jirvan.dates.Month;
import com.jirvan.dates.Second;
import com.jirvan.jidbc.lang.MultipleRowsRuntimeException;
import com.jirvan.util.DatabaseType;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Vector;

import static com.jirvan.jidbc.internal.JidbcInternalUtils.*;

public class InsertHandler {

    public static Long insert(Connection connection, Object row, String columnToReturn) {

        // Get table def and get auto generated id if appropriate
        TableDef tableDef = TableDef.getTableDefForRowClass(row.getClass());
        DatabaseType databaseType = DatabaseType.get(connection);
        if (tableDef.generatorSequence != null
            && databaseType != DatabaseType.sqlite
            && (tableDef.databasesToIgnoreGeneratorSequenceFor == null
                || databaseType.isNotOneOf(tableDef.databasesToIgnoreGeneratorSequenceFor))) {
            if (tableDef.pkColumnDefs.size() != 1) {
                throw new RuntimeException(String.format("Cannot generate id for row class %s as it does not have exactly one id field (it has %d)", tableDef.rowClass.getName(), tableDef.pkColumnDefs.size()));
            } else if (tableDef.pkColumnDefs.get(0).getValue(row) == null) {
                tableDef.pkColumnDefs.get(0).setValue(row, SequenceHandler.takeSequenceNextVal(connection, tableDef.generatorSequence));
            }
        }

        // Build insert sql and parameters
        StringBuilder columNamesStringBuilder = new StringBuilder();
        StringBuilder paramPlaceHoldersStringBuilder = new StringBuilder();
        final Vector<Object> parameterValues = new Vector<Object>();
        for (final ColumnDef columnDef : tableDef.columnDefs) {
            Object value = columnDef.getValue(row);
            if (value != null) {
                if (columNamesStringBuilder.length() != 0) {
                    columNamesStringBuilder.append(",");
                    paramPlaceHoldersStringBuilder.append(",");
                }
                columNamesStringBuilder.append(columnDef.columnName);
                paramPlaceHoldersStringBuilder.append("?");

                AttributeValueHandler.performWithValue(columnDef.attributeType, value,
                                                       new AttributeValueHandler.ValueAction() {

                                                           public void performWith(String value) {
                                                               parameterValues.add(value);
                                                           }

                                                           public void performWith(Integer value) {
                                                               parameterValues.add(value);
                                                           }

                                                           public void performWith(Long value) {
                                                               parameterValues.add(value);
                                                           }

                                                           public void performWith(BigDecimal value) {
                                                               parameterValues.add(value);
                                                           }

                                                           public void performWith(Boolean value) {
                                                               parameterValues.add(value);
                                                           }

                                                           public void performWith(java.util.Date value) {
                                                               parameterValues.add(value == null ? null : new Timestamp(value.getTime()));
                                                           }

                                                           public void performWith(Month value) {
                                                               parameterValues.add(value == null ? null : value.toString());
                                                           }

                                                           public void performWith(Day value) {
                                                               if (columnDef.storeAsTimestamp) {
                                                                   parameterValues.add(value == null ? null : new Timestamp(value.getDate().getTime()));
                                                               } else {
                                                                   parameterValues.add(value == null ? null : value.toString());
                                                               }
                                                           }

                                                           public void performWith(LocalDate value) {
                                                               if (columnDef.storeAsTimestamp) {
                                                                   parameterValues.add(value == null ? null : Timestamp.valueOf(value.atStartOfDay()).getTime());
                                                               } else {
                                                                   parameterValues.add(value == null ? null : value.toString());
                                                               }
                                                           }

                                                           public void performWith(LocalDateTime value) {
                                                               if (columnDef.storeAsTimestamp) {
                                                                   parameterValues.add(value == null ? null : Timestamp.valueOf(value).getTime());
                                                               } else {
                                                                   parameterValues.add(value == null ? null : value.toString());
                                                               }
                                                           }

                                                           public void performWith(Hour value) {
                                                               if (columnDef.storeAsTimestamp) {
                                                                   parameterValues.add(value == null ? null : new Timestamp(value.getDate().getTime()));
                                                               } else {
                                                                   parameterValues.add(value == null ? null : value.toString());
                                                               }
                                                           }

                                                           public void performWith(Minute value) {
                                                               if (columnDef.storeAsTimestamp) {
                                                                   parameterValues.add(value == null ? null : new Timestamp(value.getDate().getTime()));
                                                               } else {
                                                                   parameterValues.add(value == null ? null : value.toString());
                                                               }
                                                           }

                                                           public void performWith(Second value) {
                                                               if (columnDef.storeAsTimestamp) {
                                                                   parameterValues.add(value == null ? null : new Timestamp(value.getDate().getTime()));
                                                               } else {
                                                                   parameterValues.add(value == null ? null : value.toString());
                                                               }
                                                           }

                                                           public void performWith(Millisecond value) {
                                                               if (columnDef.storeAsTimestamp) {
                                                                   parameterValues.add(value == null ? null : new Timestamp(value.getDate().getTime()));
                                                               } else {
                                                                   parameterValues.add(value == null ? null : value.toString());
                                                               }
                                                           }

                                                           public void performWith(ZonedDateTime value) {
                                                               if (columnDef.storeAsTimestamp) {
                                                                   parameterValues.add(value == null ? null : new Timestamp(value.toInstant().getEpochSecond() * 1000L));
                                                               } else {
                                                                   parameterValues.add(value == null ? null : value.toString());
                                                               }
                                                           }

                                                           public void performWith(Enum value) {
                                                               parameterValues.add(value == null ? null : value.name());
                                                           }

                                                           public void performWith(byte[] value) {
                                                               parameterValues.add(value);
                                                           }

                                                       });
            }
        }
        String sql = columnToReturn == null
                     ? String.format("insert into %s (%s) values (%s)",
                                     tableDef.tableName,
                                     columNamesStringBuilder.toString(),
                                     paramPlaceHoldersStringBuilder.toString())
                     : String.format("insert into %s (%s) values (%s) returning %s",
                                     tableDef.tableName,
                                     columNamesStringBuilder.toString(),
                                     paramPlaceHoldersStringBuilder.toString(),
                                     columnToReturn);

        // Insert the object
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            try {
                for (int i = 0; i < parameterValues.size(); i++) {
                    setObject(statement, i + 1, parameterValues.elementAt(i));
                }

                if (columnToReturn == null) {
                    if (tableDef.pkColumnDefs.size() == 1 && databaseType == DatabaseType.sqlite && tableDef.ifSQLiteUseAutoincrement) {
                        long returnValue = (long) statement.executeUpdate();
                        long lastId = QueryForHandler.queryFor_Long(connection, true, "select last_insert_rowid()");
                        tableDef.pkColumnDefs.get(0).setValue(row, lastId);
                        return returnValue;
                    } else {
                        return (long) statement.executeUpdate();
                    }
                } else {
                    ResultSet rset = statement.executeQuery();
                    try {
                        Long primaryKeyValue;
                        if (rset.next()) {
                            primaryKeyValue = columnToReturn == null ? null : rset.getLong(columnToReturn);
                            if (rset.next()) {
                                throw new MultipleRowsRuntimeException(String.format("Error inserting row into %s", tableDef.tableName));
                            }
                        } else {
                            throw new RuntimeException(String.format("Error inserting row into %s", tableDef.tableName));
                        }
                        return primaryKeyValue;
                    } finally {
                        rset.close();
                    }
                }


            } finally {
                statement.close();
            }
        } catch (Throwable t) {
            throw new RuntimeException(String.format("Insert into %s table failed (%s)", tableDef.tableName, t.getMessage()), t);
        }
    }

}
