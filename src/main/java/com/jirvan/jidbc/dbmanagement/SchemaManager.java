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

package com.jirvan.jidbc.dbmanagement;

import com.jirvan.jidbc.Jidbc;
import com.jirvan.jidbc.JidbcConnection;
import com.jirvan.lang.MessageException;
import com.jirvan.lang.SQLRuntimeException;
import com.jirvan.util.Strings;
import com.jirvan.util.Utl;

import javax.sql.DataSource;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SchemaManager {

    public static String getSchemaVersion(DataSource dataSource) {
        try {
            return Jidbc.queryFor_String(dataSource, "select schema_version from schema_variables");
        } catch (Throwable t) {
            if (t.getMessage().contains("relation \"schema_variables\" does not exist")
                || t.getMessage().contains("no such table: schema_variables")) {
                return null;
            } else {
                throw new RuntimeException(t);
            }
        }
    }

    public static void checkNoTablesExist(DataSource dataSource) {
        try {
            Connection connection = dataSource.getConnection();
            try {
                checkNoTablesExist(connection);
            } finally {
                connection.close();
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public static void checkNoTablesExist(JidbcConnection jidbc) {
        checkNoTablesExist(jidbc.getJdbcConnection());
    }

    public static void checkNoTablesExist(Connection connection) {
        try {

            // Check database is supported
            String databaseProductName = Jidbc.getDatabaseProductName(connection);
            if (!"PostgreSQL".equals(databaseProductName)) {
                throw new RuntimeException(String.format("At this point the only database that has been tested is PostgreSQL (current database is %s)", databaseProductName));
            }

            // Check for tables
            List<String> tables = new ArrayList<String>();
            ResultSet resultSet = connection.getMetaData().getTables(null, null, null, new String[]{"TABLE"});
            try {
                while (resultSet.next()) {
                    tables.add(resultSet.getString("TABLE_NAME"));
                }
            } finally {
                resultSet.close();
            }
            if (tables.size() > 0) {
                throw new RuntimeException(String.format("Tables exist (%s)", Strings.join(tables, ',')));
            }

        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public static void upgrade(String currentSchemaVersion, String expectedNewVersion, List<SchemaUpgrader> schemaUpgraders) {
        upgrade(null, currentSchemaVersion, expectedNewVersion, schemaUpgraders);
    }

    public static void upgrade(OutputStream outputStream, String currentSchemaVersion, String expectedNewVersion, List<SchemaUpgrader> schemaUpgraders) {

        PrintWriter outputPrintWriter = outputStream == null ? null : new PrintWriter(outputStream, true);

        String upgradeToVersion = schemaUpgraders.get(schemaUpgraders.size() - 1).getToVersion();
        if (!expectedNewVersion.equals(upgradeToVersion)) {
            throw new RuntimeException(String.format("DbUpgrader version (%s) is not the same as the toVersion of the final schemaUpgrader (%s)",
                                                     upgradeToVersion, expectedNewVersion));
        }

        if (upgradeToVersion.equals(currentSchemaVersion)) {
            throw new MessageException(String.format("Database schema is already at version \"%s\"", upgradeToVersion));
        } else {

            List<SchemaUpgrader> requiredSchemaUpgraders = new ArrayList<>();
            for (SchemaUpgrader schemaUpgrader : schemaUpgraders) {
                if (Utl.areEqual(schemaUpgrader.getFromVersion(), currentSchemaVersion) || requiredSchemaUpgraders.size() > 0) {
                    requiredSchemaUpgraders.add(schemaUpgrader);
                }
            }
            if (requiredSchemaUpgraders.size() == 0) {
                throw new MessageException(String.format("Unrecognized schema version \"%s\"", currentSchemaVersion));
            }

            for (SchemaUpgrader schemaUpgrader : requiredSchemaUpgraders) {
                if (outputStream != null) {
                    if (schemaUpgrader.getFromVersion() == null) {
                        outputPrintWriter.printf("\nCreating db schema at version %s\n",
                                                 schemaUpgrader.getToVersion());
                    } else {
                        outputPrintWriter.printf("Upgrading db schema from version %s to %s\n",
                                                 schemaUpgrader.getFromVersion(), schemaUpgrader.getToVersion());
                    }
                }
                schemaUpgrader.upgrade(outputPrintWriter);
            }
        }
    }

}
