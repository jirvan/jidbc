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

import java.math.*;
import java.sql.*;
import java.util.*;

public class InsertHandler {

    public static Long insert(Connection connection, Object row, String columnToReturn) {

        TableDef tableDef = TableDef.getForRowClass(row.getClass());

        // Build insert sql and parameters
        StringBuilder columNamesStringBuilder = new StringBuilder();
        StringBuilder paramPlaceHoldersStringBuilder = new StringBuilder();
        final Vector<Object> parameterValues = new Vector<Object>();
        for (ColumnDef columnDef : tableDef.columnDefMap.values()) {
            Object value = null;
            try {
                value = columnDef.field.get(row);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            if (columNamesStringBuilder.length() != 0) {
                columNamesStringBuilder.append(",");
                paramPlaceHoldersStringBuilder.append(",");
            }
            columNamesStringBuilder.append(columnDef.columnName);
            paramPlaceHoldersStringBuilder.append("?");

            FieldValueHandler.performWithValue(columnDef.field.getType(), value,
                                               new FieldValueHandler.ValueAction() {

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

                                                   public void performWith(Day value) {
                                                       parameterValues.add(value == null ? null : new Timestamp(value.getDate().getTime()));
                                                   }

                                               });
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
                    statement.setObject(i + 1, parameterValues.elementAt(i));
                }

                if (columnToReturn == null) {
                    return (long) statement.executeUpdate();
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
