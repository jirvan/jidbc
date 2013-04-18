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
import com.jirvan.jidbc.*;
import com.jirvan.jidbc.lang.*;
import com.jirvan.util.*;

import java.math.*;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class UpdateHandler {

    public static void update(Connection connection, Object row, String[] idColumns) {

        TableDef tableDef = TableDef.getForRowClass(row.getClass());

        // Build update sql and parameters
        StringBuilder columnSetClausesStringBuilder = new StringBuilder();
        StringBuilder columnEqualityClausesStringBuilder = new StringBuilder();
        final List<Object> columnSetParameterValues = new ArrayList<Object>();
        final List<Object> columnEqualityParameterValues = new ArrayList<Object>();
        final List<String> idColumnsRemaining = new ArrayList<String>(Arrays.asList(idColumns));
        for (ColumnDef columnDef : tableDef.columnDefMap.values()) {
            if (Strings.in(columnDef.columnName, idColumnsRemaining.toArray(new String[idColumnsRemaining.size()]))) {
                processPkColumn(row, columnEqualityClausesStringBuilder, columnEqualityParameterValues, columnDef);
                idColumnsRemaining.remove(columnDef.columnName);
            } else {
                processNonPkColumn(row, columnSetClausesStringBuilder, columnSetParameterValues, columnDef);
            }
        }
        if (idColumnsRemaining.size() > 0) {
            throw new RuntimeException(String.format("ID column %s does not exist in %s", idColumnsRemaining.get(0), row.getClass().getName()));
        }
        String sql = String.format("update %s set\n   %s\nwhere %s",
                                   tableDef.tableName,
                                   columnSetClausesStringBuilder.toString(),
                                   columnEqualityClausesStringBuilder.toString());

        // Update the object
        try {
            PreparedStatement statement = connection.prepareStatement(sql);
            try {
                int paramIndex = 0;
                for (int i = 0; i < columnSetParameterValues.size(); i++) {
                    statement.setObject(++paramIndex, columnSetParameterValues.get(i));
                }
                for (int i = 0; i < columnEqualityParameterValues.size(); i++) {
                    statement.setObject(++paramIndex, columnEqualityParameterValues.get(i));
                }
                int count = statement.executeUpdate();
                if (count == 0) {
                    throw new NotFoundRuntimeException("Update failed - row not found");
                }
                if (count > 1) {
                    throw new MultipleRowsRuntimeException("Update failed - more than one row updated");
                }
            } finally {
                statement.close();
            }
        } catch (Throwable t) {
            throw new RuntimeException(String.format("Insert into %s table failed (%s)", tableDef.tableName, t.getMessage()), t);
        }
    }

    private static void processNonPkColumn(Object row, StringBuilder columnSetClausesStringBuilder, final List<Object> parameterValues, ColumnDef columnDef) {
        Object value = null;
        try {
            value = columnDef.field.get(row);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        if (columnSetClausesStringBuilder.length() != 0) {
            columnSetClausesStringBuilder.append(",\n   ");
        }
        columnSetClausesStringBuilder.append(columnDef.columnName);
        columnSetClausesStringBuilder.append(" = ?");

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

                                               public void performWith(Date value) {
                                                   parameterValues.add(value == null ? null : new Timestamp(value.getTime()));
                                               }

                                               public void performWith(Day value) {
                                                   parameterValues.add(value == null ? null : new Timestamp(value.getDate().getTime()));
                                               }

                                           });

    }

    private static void processPkColumn(Object row, StringBuilder columnEqualityClausesStringBuilder, final List<Object> parameterValues, ColumnDef columnDef) {
        Object value = null;
        try {
            value = columnDef.field.get(row);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        if (columnEqualityClausesStringBuilder.length() != 0) {
            columnEqualityClausesStringBuilder.append("\n  and ");
        }
        columnEqualityClausesStringBuilder.append(columnDef.columnName);
        columnEqualityClausesStringBuilder.append(" = ?");

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

                                               public void performWith(Date value) {
                                                   parameterValues.add(value == null ? null : new Timestamp(value.getTime()));
                                               }

                                               public void performWith(Day value) {
                                                   parameterValues.add(value == null ? null : new Timestamp(value.getDate().getTime()));
                                               }

                                           });

    }

    private static void processNonPkzzzColumn(Object row, StringBuilder columNamesStringBuilder, StringBuilder paramPlaceHoldersStringBuilder, final Vector<Object> parameterValues, ColumnDef columnDef) {
        Object value = null;
        try {
            value = columnDef.field.get(row);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        if (columNamesStringBuilder.length() != 0) {
            columNamesStringBuilder.append(",\n   ");
            paramPlaceHoldersStringBuilder.append(",");
        }
        columNamesStringBuilder.append(columnDef.columnName);
        columNamesStringBuilder.append(" = ?");
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

                                               public void performWith(Date value) {
                                                   parameterValues.add(value == null ? null : new Timestamp(value.getTime()));
                                               }

                                               public void performWith(Day value) {
                                                   parameterValues.add(value == null ? null : new Timestamp(value.getDate().getTime()));
                                               }

                                           });
    }

}
