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

import com.jirvan.jidbc.*;
import com.jirvan.util.*;

import javax.sql.*;

public abstract class SchemaMigrator {

    private DataSource dataSource;
    private String fromVersion;
    private String toVersion;


    protected SchemaMigrator(DataSource dataSource, String fromVersion, String toVersion) {
        this.dataSource = dataSource;
        this.fromVersion = fromVersion;
        this.toVersion = toVersion;
    }

    protected abstract void performMigration(JidbcConnection jidbcConnection);

    public void migrate() {
        String currentSchemaVersion = SchemaManager.getSchemaVersion(dataSource);
        if (fromVersion == null) {
            if (currentSchemaVersion == null) {
                performBootstrapMigration();
            } else {
                throw new RuntimeException(String.format("Cannot \"bootstrap\" to schema version \"%s\", schema is already at version %s", toVersion, currentSchemaVersion));
            }
        } else {
            if (currentSchemaVersion == null) {
                throw new RuntimeException(String.format("Cannot upgrade from schema version \"%s\", schema is not currently at ANY version", fromVersion));
            } else if (!currentSchemaVersion.equals(fromVersion)) {
                throw new RuntimeException(String.format("Cannot upgrade from schema version \"%s\", schema is already at version %s", toVersion, currentSchemaVersion));
            } else {
                performNormalMigration();
            }
        }
    }

    private void performBootstrapMigration() {

        // Check user has no existing tables etc
        String[] existingTables = JidbcDbAdmin.getCurrentUsersTables(dataSource);
        if (existingTables.length > 0) {
            throw new RuntimeException(String.format("User %s already has some tables (must be none for a \"bootstrap\" migration).  The tables are: %s.",
                                                     JidbcDbAdmin.getCurrentUser(dataSource),
                                                     Strings.join(existingTables, ',')));
        }
        String[] existingViews = JidbcDbAdmin.getCurrentUsersViews(dataSource);
        if (existingViews.length > 0) {
            throw new RuntimeException(String.format("User %s already has some views (must be none for a \"bootstrap\" migration).  The views are: %s.",
                                                     JidbcDbAdmin.getCurrentUser(dataSource),
                                                     Strings.join(existingViews, ',')));
        }
        String[] existingSequences = JidbcDbAdmin.getCurrentUsersViews(dataSource);
        if (existingSequences.length > 0) {
            throw new RuntimeException(String.format("User %s already has some sequences (must be none for a \"bootstrap\" migration).  The sequences are: %s.",
                                                     JidbcDbAdmin.getCurrentUser(dataSource),
                                                     Strings.join(existingSequences, ',')));
        }

        // Perform the migration
        String interimVersion = "bootstrapping to " + toVersion;
        createSchemaVariablesTable(dataSource, interimVersion);
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            performMigration(jidbc);

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
        String currentSchemaVersion = SchemaManager.getSchemaVersion(dataSource);
        if (!interimVersion.equals(currentSchemaVersion)) {
            throw new RuntimeException(String.format("Unexpected error on finalization, expected schema version to be \"%s\" but it was \"%s\"", interimVersion, currentSchemaVersion));
        }
        Jidbc.executeUpdate(dataSource, "update schema_variables set schema_version = ?", toVersion);

    }

    private void performNormalMigration() {
        String interimVersion = String.format("\"%s\" migrating to \"%s\"", fromVersion, toVersion);
        Jidbc.executeUpdate(dataSource, "update schema_variables set schema_version = ?", interimVersion);
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            performMigration(jidbc);

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
        String currentSchemaVersion = SchemaManager.getSchemaVersion(dataSource);
        if (!interimVersion.equals(currentSchemaVersion)) {
            throw new RuntimeException(String.format("Unexpected error on finalization, expected schema version to be \"%s\" but it was \"%s\"", interimVersion, currentSchemaVersion));
        }
        Jidbc.executeUpdate(dataSource, "update schema_variables set schema_version = ?", toVersion);
    }

    private void createSchemaVariablesTable(DataSource dataSource, String initialVersion) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            jidbc.executeUpdate("create table schema_variables (\n" +
                                "   single_row_enforcer       numeric(2) default 42    not null,\n" +
                                "   schema_version            varchar(300)             not null,\n" +
                                "constraint schema_variables_pk primary key (single_row_enforcer),\n" +
                                "constraint no_more_than_one_row_chk\n" +
                                "   check (\n" +
                                "      single_row_enforcer = 42\n" +
                                "   )\n" +
                                ")");

            jidbc.executeUpdate("insert into schema_variables (schema_version) values (?)", initialVersion);

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    protected void executeScript(JidbcConnection jidbc, String scriptRelativePath) {
        String script = Io.getResourceFileString(this.getClass(), scriptRelativePath);
        for (String sql : script.replaceAll("(?m)^\\s+--.*$", "")
                                .replaceAll("^\\s*\\n+", "")
                                .replaceAll("(?m);\\s*\\n\\s*", ";\n")
                                .split("(?m); *\\n")) {
            jidbc.executeUpdate(sql);
        }
    }

}
