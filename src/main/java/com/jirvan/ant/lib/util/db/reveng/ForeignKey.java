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

public class ForeignKey {

    public String catalogName;
    public String schemaName;
    public String tableName;
    public String foreignKeyName;
    public String referencedTableName;
    public ColumnNamePair[] columnNamePairs;

    public static ForeignKey[] getForeignKeysForTable(DatabaseMetaData databaseMetaData, Table table) throws SQLException {
        if ("ACCESS".equalsIgnoreCase(databaseMetaData.getDatabaseProductName())) {
            return new ForeignKey[0];
        } else {
            ResultSet rset = databaseMetaData.getImportedKeys(table.catalogName, table.schemaName, table.tableName);
            try {
                Map<String, PlaceHolder> foreignKeysMap = new TreeMap<String, PlaceHolder>();
                while (rset.next()) {
                    String foreignKeyName = rset.getString("FK_NAME");
                    PlaceHolder placeHolder = foreignKeysMap.get(foreignKeyName);
                    if (placeHolder == null) {
                        placeHolder = new PlaceHolder();
                        placeHolder.referencedTableName = rset.getString("PKTABLE_NAME");
                        placeHolder.columnNamePairVector = new Vector<ColumnNamePair>();
                        foreignKeysMap.put(foreignKeyName, placeHolder);
                    }
                    placeHolder.columnNamePairVector.add(new ColumnNamePair(rset.getString("FKCOLUMN_NAME"),
                                                                            rset.getString("PKCOLUMN_NAME")));
                }

                String[] foreignKeyNames = foreignKeysMap.keySet().toArray(new String[foreignKeysMap.size()]);
                ForeignKey[] foreignKeys = new ForeignKey[foreignKeyNames.length];
                for (int i = 0; i < foreignKeys.length; i++) {
                    foreignKeys[i] = new ForeignKey();
                    foreignKeys[i].catalogName = table.catalogName;
                    foreignKeys[i].schemaName = table.schemaName;
                    foreignKeys[i].tableName = table.tableName;
                    foreignKeys[i].foreignKeyName = foreignKeyNames[i];
                    PlaceHolder placeHolder = foreignKeysMap.get(foreignKeyNames[i]);
                    foreignKeys[i].referencedTableName = placeHolder.referencedTableName;
                    foreignKeys[i].columnNamePairs = placeHolder.columnNamePairVector.toArray(new ColumnNamePair[placeHolder.columnNamePairVector.size()]);
                }
                return foreignKeys;

            } finally {
                rset.close();
            }
        }
    }

    public static class PlaceHolder {
        public String referencedTableName;
        public Vector<ColumnNamePair> columnNamePairVector;
    }

    public static class ColumnNamePair {

        public String columnName;
        public String referencedColumnName;

        public ColumnNamePair(String columnName, String referencedColumnName) {
            this.columnName = columnName;
            this.referencedColumnName = referencedColumnName;
        }

    }

}