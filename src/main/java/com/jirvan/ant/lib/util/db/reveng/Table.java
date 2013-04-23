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

public class Table {

    public String catalogName;
    public String schemaName;
    public String tableName;
    public Column[] columns;
    public PrimaryKey primaryKey;
    public UniqueKey[] uniqueKeys;
    public NonUniqueIndex[] nonUniqueIndexes;
    public ForeignKey[] foreignKeys;
    public CheckConstraint[] checkConstraints;

    public Long numRows;

    public int maxColumnNameLength;

    public static Table[] getTablesForSchema(Connection connection,
                                             DatabaseMetaData databaseMetaData,
                                             Schema schema,
                                             String[] tablesToInclude,
                                             DatatypeMap[] mapdatatypes,
                                             boolean noCheckConstaints,
                                             boolean includeStuffForDocs) throws SQLException {
        ResultSet rset = databaseMetaData.getTables(schema.catalogName, schema.schemaName, "%", new String[]{"TABLE"});
        try {
            Vector<Table> tablesVector = new Vector<Table>();
            while (rset.next()) {
                String tableName = rset.getString("TABLE_NAME");
                if (tablesToInclude == null || Strings.in(tableName, tablesToInclude)) {
                    Table table = Table.get(connection, databaseMetaData, schema, tableName, mapdatatypes, noCheckConstaints);
                    if (includeStuffForDocs) {
                        addStuffForDocs(connection, table);
                    }
                    tablesVector.add(table);
                }
            }
            return tablesVector.toArray(new Table[tablesVector.size()]);
        } finally {
            rset.close();
        }
    }

    public static void addStuffForDocs(Connection connection,
                                       Table table) throws SQLException {

        // Build SQL
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < table.columns.length; i++) {
            Column column = table.columns[i];
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
        if (buf.length() > 0) {
            buf.append("\nfrom " + table.tableName);
            String sql = buf.toString();
            //System.out.printf("\n\nsql:\n%s", sql);
            PreparedStatement stmt = connection.prepareStatement(sql);
            ResultSet rset = null;
            try {
                rset = stmt.executeQuery();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            try {
                if (rset.next()) {
                    int i = 0;
                    table.numRows = rset.getLong(++i);
                    for (Column column : table.columns) {
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

    }

    public static Table get(Connection connection,
                            DatabaseMetaData databaseMetaData,
                            Schema schema,
                            String tableName,
                            DatatypeMap[] mapdatatypes,
                            boolean noCheckConstaints) throws SQLException {
        if (schema == null) throw new RuntimeException("schema cannot be null");
        Table table = new Table();
        table.catalogName = schema.catalogName;
        table.schemaName = schema.schemaName;
        table.tableName = tableName;
        table.columns = Column.getTableColumns(databaseMetaData, table, mapdatatypes);
        table.maxColumnNameLength = 0;
        for (Column column : table.columns) {
            table.maxColumnNameLength = Math.max(table.maxColumnNameLength, column.columnName.length());
        }
        table.primaryKey = PrimaryKey.getPrimaryKeyForTable(databaseMetaData, table);
        table.uniqueKeys = UniqueKey.getUniqueKeysForTable(databaseMetaData, table);
        table.nonUniqueIndexes = NonUniqueIndex.getNonUniqueIndexsForTable(databaseMetaData, table);
        table.foreignKeys = ForeignKey.getForeignKeysForTable(databaseMetaData, table);
        if (!noCheckConstaints) {
            table.checkConstraints = CheckConstraint.getCheckConstraintsForTable(connection, databaseMetaData, table);
        }
        return table;
    }

    public static class Info {

        public Long maxWidth;
        public Long numDistinct;
        public Vector<String> examples;

    }

}
