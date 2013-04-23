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

public class UniqueKey {

    public String schemaName;
    public String tableName;
    public String uniqueKeyName;
    public String[] columnNames;

    public static UniqueKey[] getUniqueKeysForTable(DatabaseMetaData databaseMetaData, Table table) throws SQLException {
        if (databaseMetaData == null) throw new RuntimeException("databaseMetaData cannot be null");
        if (table == null) throw new RuntimeException("table cannot be null");

        ResultSet rset = databaseMetaData.getIndexInfo(table.catalogName, table.schemaName, table.tableName, true, true);

        SortedMap<String, SortedMap<Integer, String>> keysSortedMap = new TreeMap<String, SortedMap<Integer, String>>();
        try {
            String primaryKeyName = table.primaryKey == null ? null : table.primaryKey.primaryKeyName;
            while (rset.next()) {
                String uniqueKeyName = rset.getString("INDEX_NAME");
//                Map columns = Jdbc.mapRow(rset);
                if (uniqueKeyName == null) {
                    System.out.printf("WARNING: unique key with a null name ignored (could be the primary key?)\n");
                } else {
                    if (!uniqueKeyName.equals(primaryKeyName)) {
                        SortedMap<Integer, String> columnsSortedMap = keysSortedMap.get(uniqueKeyName);
                        if (columnsSortedMap == null) {
                            columnsSortedMap = new TreeMap<Integer, String>();
                            keysSortedMap.put(uniqueKeyName, columnsSortedMap);
                        }
                        columnsSortedMap.put(rset.getInt("ORDINAL_POSITION"),
                                             rset.getString("COLUMN_NAME"));
                    }
                }
            }
            String[] uniqueKeyNames = keysSortedMap.keySet().toArray(new String[keysSortedMap.size()]);
            UniqueKey[] uniqueKeys = new UniqueKey[uniqueKeyNames.length];
            for (int i = 0; i < uniqueKeys.length; i++) {
                UniqueKey uniqueKey = new UniqueKey();
                uniqueKey.schemaName = table.schemaName;
                uniqueKey.tableName = table.tableName;
                uniqueKey.uniqueKeyName = uniqueKeyNames[i];
                Collection<String> columnsCollection = keysSortedMap.get(uniqueKeyNames[i]).values();
                uniqueKey.columnNames = columnsCollection.toArray(new String[columnsCollection.size()]);
                uniqueKeys[i] = uniqueKey;
            }
            return uniqueKeys;
        } finally {
            rset.close();
        }

    }

}