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
    private boolean migrateInASingleTransaction;
    private String fromVersion;
    private String toVersion;

    /**
     * @param dataSource                  the data source
     * @param migrateInASingleTransaction Indicates whether or not to do the migration as a single transaction.  This
     *                                    is really only here to support databases like Oracle that only allow DML
     *                                    statements to be part of a transaction.  In Oracle DDL statements (table creations,
     *                                    alterations etc) force an implicit commit.  Setting this to false really just means that the
     *                                    schema version will be set to an "in between" value (e.g. "1.0" migrating to "1.1") so that
     *                                    if the migration fails part way and is partially committed due to implicit commits,
     *                                    this will be indicated by the "in between" value of the schema version.
     * @param fromVersion                 from versiion
     * @param toVersion                   to version
     */
    protected SchemaMigrator(DataSource dataSource, boolean migrateInASingleTransaction, String fromVersion, String toVersion) {
        this.dataSource = dataSource;
        this.migrateInASingleTransaction = migrateInASingleTransaction;
        this.fromVersion = fromVersion;
        this.toVersion = toVersion;
    }

    public String getFromVersion() {
        return fromVersion;
    }

    public String getToVersion() {
        return toVersion;
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
                throw new RuntimeException(String.format("Cannot upgrade from schema version \"%s\", schema is currently at version \"%s\"", fromVersion, currentSchemaVersion));
            } else {
                performNormalMigration();
            }
        }
    }

    private void performBootstrapMigration() {

        JidbcDbAdmin.verifyNoTablesViewsOrSequencesExistOwnedByCurrentUser(dataSource);

        // Perform the migration
        String interimVersion = "bootstrapping to " + toVersion;
        if (!migrateInASingleTransaction) {
            createSchemaVariablesTable(dataSource, interimVersion);
        }
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            createSchemaVariablesTable(jidbc, interimVersion);

            performMigration(jidbc);

            if (migrateInASingleTransaction) {
                jidbc.executeUpdate("update schema_variables set schema_version = ?", toVersion);
            }

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
        if (!migrateInASingleTransaction) {
            String currentSchemaVersion = SchemaManager.getSchemaVersion(dataSource);
            if (!interimVersion.equals(currentSchemaVersion)) {
                throw new RuntimeException(String.format("Unexpected error on finalization, expected schema version to be \"%s\" but it was \"%s\"", interimVersion, currentSchemaVersion));
            }
            Jidbc.executeUpdate(dataSource, "update schema_variables set schema_version = ?", toVersion);
        }

    }

    private void performNormalMigration() {
        String interimVersion = String.format("\"%s\" migrating to \"%s\"", fromVersion, toVersion);
        if (!migrateInASingleTransaction) {
            Jidbc.executeUpdate(dataSource, "update schema_variables set schema_version = ?", interimVersion);
        }
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            performMigration(jidbc);

            if (migrateInASingleTransaction) {
                Jidbc.executeUpdate(dataSource, "update schema_variables set schema_version = ?", toVersion);
            }

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
        if (!migrateInASingleTransaction) {
            String currentSchemaVersion = SchemaManager.getSchemaVersion(dataSource);
            if (!interimVersion.equals(currentSchemaVersion)) {
                throw new RuntimeException(String.format("Unexpected error on finalization, expected schema version to be \"%s\" but it was \"%s\"", interimVersion, currentSchemaVersion));
            }
            Jidbc.executeUpdate(dataSource, "update schema_variables set schema_version = ?", toVersion);
        }
    }

    private void createSchemaVariablesTable(DataSource dataSource, String initialVersion) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {
            createSchemaVariablesTable(jidbc, initialVersion);
            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    private void createSchemaVariablesTable(JidbcConnection jidbc, String initialVersion) {
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

    protected void assignPrivilegeToRoles(JidbcConnection jidbc, String privilegeName, String firstRole, String... theRestOfTheRoles) {

        String[] roles = Utl.merge(firstRole, theRestOfTheRoles);

        Object[] parameters = Utl.merge(Object.class, jidbc.queryFor_Long("select privilege_id from application_privileges where name = ?", privilegeName),
                                        roles);

        jidbc.executeUpdate(String.format("insert into role_privileges (role_id, privilege_id)\n" +
                                          "select role_id, ?\n" +
                                          "from roles\n" +
                                          "where name in (%s)", Jdbc.parameterPlaceHolderString(roles)),
                            parameters);
    }

    protected void insertApplicationPrivilege(JidbcConnection jidbc, String name, String description) {
        jidbc.executeUpdate("insert into application_privileges (privilege_id, name, description) values (?,?,?)",
                            jidbc.takeSequenceNextVal("common_id_sequence"),
                            name, description);
    }

    protected void insertRole(JidbcConnection jidbc, String name, String description) {
        jidbc.executeUpdate("insert into roles (role_id, name, description) values (?,?,?)",
                            jidbc.takeSequenceNextVal("common_id_sequence"), name, description);
    }

}
