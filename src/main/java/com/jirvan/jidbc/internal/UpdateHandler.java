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
import com.jirvan.jidbc.lang.NotFoundRuntimeException;
import com.jirvan.lang.*;

import java.math.*;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class UpdateHandler extends AbstractPkWhereClauseHandler {

    public static void update(Connection connection, Object row) {
        try {

            TableDef tableDef = TableDef.getForRowClass(row.getClass());

            // Build update sql and parameters
            StringBuilder columnSetClausesStringBuilder = new StringBuilder();
            final List<Object> columnSetParameterValues = new ArrayList<Object>();
            for (ColumnDef columnDef : tableDef.nonPkColumnDefs) {
                processNonPkColumn(row, columnSetClausesStringBuilder, columnSetParameterValues, columnDef);
            }


            WhereClause whereClause = new WhereClause(tableDef, row);


            String sql = String.format("update %s set\n   %s\n%s",
                                       tableDef.tableName,
                                       columnSetClausesStringBuilder.toString(),
                                       whereClause.sql);

            // Update the object
            PreparedStatement statement = connection.prepareStatement(sql);
            try {
                int paramIndex = 0;
                for (int i = 0; i < columnSetParameterValues.size(); i++) {
                    statement.setObject(++paramIndex, columnSetParameterValues.get(i));
                }
                for (int i = 0; i < whereClause.parameterValues.size(); i++) {
                    statement.setObject(++paramIndex, whereClause.parameterValues.get(i));
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

        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    private static void processNonPkColumn(Object row, StringBuilder columnSetClausesStringBuilder, final List<Object> parameterValues, ColumnDef columnDef) {
        Object value = columnDef.getValue(row);
        if (columnSetClausesStringBuilder.length() != 0) {
            columnSetClausesStringBuilder.append(",\n   ");
        }
        columnSetClausesStringBuilder.append(columnDef.columnName);
        columnSetClausesStringBuilder.append(" = ?");

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

                                                   public void performWith(Date value) {
                                                       parameterValues.add(value == null ? null : new Timestamp(value.getTime()));
                                                   }

                                                   public void performWith(Day value) {
                                                       parameterValues.add(value == null ? null : new Timestamp(value.getDate().getTime()));
                                                   }

                                               });

    }

}
