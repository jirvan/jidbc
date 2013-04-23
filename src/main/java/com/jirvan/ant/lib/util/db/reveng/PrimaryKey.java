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

import java.sql.*;
import java.util.*;

public class PrimaryKey {

    public String schemaName;
    public String tableName;
    public String primaryKeyName;
    public String[] columnNames;

    public static PrimaryKey getPrimaryKeyForTable(DatabaseMetaData databaseMetaData, Table table) throws SQLException {
        if (databaseMetaData == null) throw new RuntimeException("databaseMetaData cannot be null");
        if (table == null) throw new RuntimeException("table cannot be null");

        if ("ACCESS".equalsIgnoreCase(databaseMetaData.getDatabaseProductName())) {
            return null;
        } else {

            ResultSet rset = databaseMetaData.getPrimaryKeys(table.catalogName, table.schemaName, table.tableName);
            SortedMap<Integer, String> sortedMap = new TreeMap<Integer, String>();
            try {
                PrimaryKey primaryKey = new PrimaryKey();
                primaryKey.schemaName = table.schemaName;
                primaryKey.tableName = table.tableName;
                while (rset.next()) {
                    if (primaryKey.primaryKeyName == null) primaryKey.primaryKeyName = rset.getString("PK_NAME");
                    sortedMap.put(rset.getInt("KEY_SEQ"),
                                  rset.getString("COLUMN_NAME"));
                }
                if (sortedMap.size() == 0) {
                    return null;
                } else {
                    primaryKey.columnNames = sortedMap.values().toArray(new String[sortedMap.size()]);
                    return primaryKey;
                }
            } finally {
                rset.close();
            }

        }
    }

}