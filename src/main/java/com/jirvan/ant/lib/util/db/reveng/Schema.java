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

import java.sql.*;
import java.util.*;

public class Schema {

    public String catalogName;
    public String schemaName;
    public Table[] tables;
    public View[] views;
    public Table[] tablesWithForeignKeys;

    public static Schema get(Connection connection,
                             DatabaseMetaData databaseMetaData,
                             String schemaName,
                             String[] tablesToInclude,
                             String[] viewsToInclude,
                             DatatypeMap[] mapdatatypes,
                             boolean noCheckConstaints,
                             boolean includeStuffForDocs) throws SQLException {
        if ("ACCESS".equalsIgnoreCase(databaseMetaData.getDatabaseProductName())) {
            return buildSchema(connection,
                               databaseMetaData,
                               tablesToInclude,
                               viewsToInclude,
                               mapdatatypes,
                               noCheckConstaints,
                               includeStuffForDocs,
                               null,
                               null);
        } else {
            if (schemaName == null) throw new RuntimeException("schemaName cannot be null");
            ResultSet rset = databaseMetaData.getSchemas();
            try {
                while (rset.next()) {
                    String thisSchemaName = rset.getString("TABLE_SCHEM");
                    String catalogName = null;
                    try {
                        catalogName = rset.getString("TABLE_CATALOG");
                    } catch (SQLException e) {
                        catalogName = null;
                    }
                    if (schemaName.equals(thisSchemaName)) {
                        return buildSchema(connection,
                                           databaseMetaData,
                                           tablesToInclude,
                                           viewsToInclude,
                                           mapdatatypes,
                                           noCheckConstaints,
                                           includeStuffForDocs,
                                           catalogName,
                                           thisSchemaName);
                    }
                }
                throw new RuntimeException("Schema \"" + schemaName + "\" not found");
            } finally {
                rset.close();
            }
        }
    }

    private static Schema buildSchema(Connection connection, DatabaseMetaData databaseMetaData, String[] tablesToInclude, String[] viewsToInclude, DatatypeMap[] mapdatatypes, boolean noCheckConstaints, boolean includeStuffForDocs, String catalogName, String thisSchemaName) throws SQLException {
        Schema schema = new Schema();
        schema.schemaName = thisSchemaName;
        schema.catalogName = catalogName;
        schema.tables = Table.getTablesForSchema(connection, databaseMetaData, schema, tablesToInclude, mapdatatypes, noCheckConstaints, includeStuffForDocs);
        schema.views = View.getViewsForSchema(connection, databaseMetaData, schema, viewsToInclude, mapdatatypes, includeStuffForDocs);
        Vector<Table> tableWithForeignKeysVector = new Vector<Table>();
        for (Table table : schema.tables) {
            if (table.foreignKeys.length > 0) {
                tableWithForeignKeysVector.add(table);
            }
        }
        schema.tablesWithForeignKeys = tableWithForeignKeysVector.toArray(new Table[tableWithForeignKeysVector.size()]);
        return schema;
    }

}
