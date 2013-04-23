/*

Copyright (c) 2008,2009 Jirvan Pty Ltd
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

package com.jirvan.ant.lib.util.db.reveng;

import java.math.*;
import java.sql.*;
import java.util.*;

public class Column {

    public String schemaName;
    public String tableName;
    public String columnName;
    public String sourceDataType;
    public String defaultValue;
    public BigDecimal columnSize;
    public BigDecimal decimalDigits;
    public boolean notNull;

    public Integer shortestLength;
    public Integer longestLength;
    public Integer numDistinctValues;

    public static Column[] getTableColumns(DatabaseMetaData databaseMetaData, Table table, DatatypeMap[] datatypeMaps) throws SQLException {
        if (databaseMetaData == null) throw new RuntimeException("databaseMetaData cannot be null");
        if (table == null) throw new RuntimeException("table cannot be null");
        ResultSet rset = databaseMetaData.getColumns(table.catalogName, table.schemaName, table.tableName, "%");
        Vector<Column> vector = new Vector<Column>();
        try {
            while (rset.next()) {
                Column column = new Column();
                column.columnName = rset.getString("COLUMN_NAME");
                column.sourceDataType = rset.getString("TYPE_NAME");
                column.columnSize = rset.getBigDecimal("COLUMN_SIZE");
                column.decimalDigits = rset.getBigDecimal("DECIMAL_DIGITS");
                DatatypeMap.DataTypeDef dataTypeDef = DatatypeMap.mappingFor(new DatatypeMap.DataTypeDef(column.sourceDataType,
                                                                                                         column.columnSize,
                                                                                                         column.decimalDigits),
                                                                             datatypeMaps);
                if (dataTypeDef != null) {
                    column.sourceDataType = dataTypeDef.getTypeName();
                    column.columnSize = dataTypeDef.getColumnSize();
                    column.decimalDigits = dataTypeDef.getDecimalDigits();
                }
                column.defaultValue = rset.getString("COLUMN_DEF");
                String isNullable = rset.getString("IS_NULLABLE");
                column.notNull = !"YES".equals(isNullable);
                vector.add(column);
            }
        } finally {
            rset.close();
        }
        return vector.toArray(new Column[vector.size()]);
    }

    public static Column[] getViewColumns(DatabaseMetaData databaseMetaData, View view, DatatypeMap[] datatypeMaps) throws SQLException {
        if (databaseMetaData == null) throw new RuntimeException("databaseMetaData cannot be null");
        if (view == null) throw new RuntimeException("view cannot be null");
        ResultSet rset = databaseMetaData.getColumns(view.catalogName, view.schemaName, view.viewName, "%");
        Vector<Column> vector = new Vector<Column>();
        try {
            while (rset.next()) {
                Column column = new Column();
                column.columnName = rset.getString("COLUMN_NAME");
                column.sourceDataType = rset.getString("TYPE_NAME");
                column.columnSize = rset.getBigDecimal("COLUMN_SIZE");
                column.decimalDigits = rset.getBigDecimal("DECIMAL_DIGITS");
                DatatypeMap.DataTypeDef dataTypeDef = DatatypeMap.mappingFor(new DatatypeMap.DataTypeDef(column.sourceDataType,
                                                                                                         column.columnSize,
                                                                                                         column.decimalDigits),
                                                                             datatypeMaps);
                if (dataTypeDef != null) {
                    column.sourceDataType = dataTypeDef.getTypeName();
                    column.columnSize = dataTypeDef.getColumnSize();
                    column.decimalDigits = dataTypeDef.getDecimalDigits();
                }
                column.defaultValue = rset.getString("COLUMN_DEF");
                String isNullable = rset.getString("IS_NULLABLE");
                column.notNull = !"YES".equals(isNullable);
                vector.add(column);
            }
        } finally {
            rset.close();
        }
        return vector.toArray(new Column[vector.size()]);
    }

}
