/*

Copyright (c) 2008, Jirvan Pty Ltd
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

import com.jirvan.ant.*;
import com.jirvan.util.*;

import java.sql.*;
import java.util.*;

public class View {

    public String catalogName;
    public String schemaName;
    public String viewName;
    public Column[] columns;

    public Long numRows;

    public int maxColumnNameLength;

    public static View[] getViewsForSchema(Connection connection,
                                           DatabaseMetaData databaseMetaData,
                                           Schema schema,
                                           String[] viewsToInclude,
                                           DatatypeMap[] mapdatatypes,
                                           boolean includeStuffForDocs) throws SQLException {
        ResultSet rset = databaseMetaData.getTables(schema.catalogName, schema.schemaName, "%", new String[]{"VIEW"});
        try {
            Vector<View> viewsVector = new Vector<View>();
            while (rset.next()) {
                String viewName = rset.getString("TABLE_NAME");
                if (viewsToInclude == null || Strings.in(viewName, viewsToInclude)) {
                    View view = View.get(databaseMetaData, schema, viewName, mapdatatypes);
                    if (includeStuffForDocs) {
                        addStuffForDocs(connection, view);
                    }
                    viewsVector.add(view);
                }
            }
            return viewsVector.toArray(new View[viewsVector.size()]);
        } finally {
            rset.close();
        }
    }

    public static void addStuffForDocs(Connection connection,
                                       View view) throws SQLException {

        // Build SQL
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < view.columns.length; i++) {
            Column column = view.columns[i];
            if ("varchar".equals(column.sourceDataType)) {
                if (buf.length() == 0) {
                    buf.append("select count(*),\n" +
                               "       min(length(" + column.columnName + ")),\n" +
                               "       max(length(" + column.columnName + ")),\n" +
                               "       count(distinct " + column.columnName + ")");
                } else {
                    buf.append(",\n       min(length(" + column.columnName + "))" +
                               ",\n       max(length(" + column.columnName + "))" +
                               ",\n       count(distinct " + column.columnName + ")");
                }
            }

        }
        buf.append("\nfrom " + view.viewName);
        String sql = buf.toString();
        System.out.printf("\n\nsql:\n%s", sql);
        PreparedStatement stmt = connection.prepareStatement(sql);
        ResultSet rset = stmt.executeQuery();
        try {
            if (rset.next()) {
                int i = 0;
                view.numRows = rset.getLong(++i);
                for (Column column : view.columns) {
                    if ("varchar".equals(column.sourceDataType)) {
                        column.shortestLength = rset.getInt(++i);
                        column.longestLength = rset.getInt(++i);
                        column.numDistinctValues = rset.getInt(++i);
                    }
                }
            } else {
                throw new RuntimeException("Expected at least one row");
            }
        } finally {
            rset.close();
        }

    }

    public static View get(DatabaseMetaData databaseMetaData,
                           Schema schema,
                           String viewName,
                           DatatypeMap[] mapdatatypes) throws SQLException {
        if (schema == null) throw new RuntimeException("schema cannot be null");
        View view = new View();
        view.catalogName = schema.catalogName;
        view.schemaName = schema.schemaName;
        view.viewName = viewName;
        view.columns = Column.getViewColumns(databaseMetaData, view, mapdatatypes);
        view.maxColumnNameLength = 0;
        for (Column column : view.columns) {
            view.maxColumnNameLength = Math.max(view.maxColumnNameLength, column.columnName.length());
        }
        return view;
    }

    public static class Info {

        public Long maxWidth;
        public Long numDistinct;
        public Vector<String> examples;

    }

}