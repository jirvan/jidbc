/*

Copyright (c) 2013,2014 Jirvan Pty Ltd
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
import com.jirvan.jidbc.JidbcDbAdmin;
import com.jirvan.util.DatabaseType;
import com.jirvan.util.Io;
import com.jirvan.util.Jdbc;
import com.jirvan.util.Utl;
import org.apache.commons.lang.WordUtils;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;

public abstract class SchemaUpgrader {

    private DataSource dataSource;
    private DatabaseType databaseType;
    private boolean upgradeInASingleTransaction;
    private String fromVersion;
    private String toVersion;

    /**
     * @param dataSource                  the data source
     * @param upgradeInASingleTransaction Indicates whether or not to do the upgrade as a single transaction.  This
     *                                    is really only here to support databases like Oracle that only allow DML
     *                                    statements to be part of a transaction.  In Oracle DDL statements (table creations,
     *                                    alterations etc) force an implicit commit.  Setting this to false really just means that the
     *                                    schema version will be set to an "in between" value (e.g. "1.0" upgrading to "1.1") so that
     *                                    if the upgrade fails part way and is partially committed due to implicit commits,
     *                                    this will be indicated by the "in between" value of the schema version.
     * @param fromVersion                 from versiion
     * @param toVersion                   to version
     */
    protected SchemaUpgrader(DataSource dataSource, boolean upgradeInASingleTransaction, String fromVersion, String toVersion) {
        this.dataSource = dataSource;
        this.databaseType = DatabaseType.get(dataSource);
        this.upgradeInASingleTransaction = upgradeInASingleTransaction;
        this.fromVersion = fromVersion;
        this.toVersion = toVersion;
    }

    public String getFromVersion() {
        return fromVersion;
    }

    public String getToVersion() {
        return toVersion;
    }

    protected abstract void performUpgrade(JidbcConnection jidbcConnection, PrintWriter output);

    public void upgrade(PrintWriter output) {
        String currentSchemaVersion = SchemaManager.getSchemaVersion(dataSource);
        if (fromVersion == null) {
            if (currentSchemaVersion == null) {
                performBootstrapUpgrade(output);
            } else {
                throw new RuntimeException(String.format("Cannot \"bootstrap\" to schema version \"%s\", schema is already at version %s", toVersion, currentSchemaVersion));
            }
        } else {
            if (currentSchemaVersion == null) {
                throw new RuntimeException(String.format("Cannot upgrade from schema version \"%s\", schema is not currently at ANY version", fromVersion));
            } else if (!currentSchemaVersion.equals(fromVersion)) {
                throw new RuntimeException(String.format("Cannot upgrade from schema version \"%s\", schema is currently at version \"%s\"", fromVersion, currentSchemaVersion));
            } else {
                performNormalUpgrade(output);
            }
        }
    }

    private void performBootstrapUpgrade(PrintWriter output) {

        JidbcDbAdmin.verifyNoTablesViewsOrSequencesExistOwnedByCurrentUser(dataSource);

        // Perform the upgrade
        String interimVersion = "bootstrapping to " + toVersion;
        if (!upgradeInASingleTransaction) {
            createSchemaVariablesTable(dataSource, output, interimVersion);
        }
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            createSchemaVariablesTable(jidbc, output, interimVersion);

            performUpgrade(jidbc, output);

            if (upgradeInASingleTransaction) {
                jidbc.executeUpdate("update schema_variables set schema_version = ?", toVersion);
            }

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
        if (!upgradeInASingleTransaction) {
            String currentSchemaVersion = SchemaManager.getSchemaVersion(dataSource);
            if (!interimVersion.equals(currentSchemaVersion)) {
                throw new RuntimeException(String.format("Unexpected error on finalization, expected schema version to be \"%s\" but it was \"%s\"", interimVersion, currentSchemaVersion));
            }
            Jidbc.executeUpdate(dataSource, "update schema_variables set schema_version = ?", toVersion);
        }

    }

    private void performNormalUpgrade(PrintWriter output) {
        String interimVersion = String.format("\"%s\" upgrading to \"%s\"", fromVersion, toVersion);
        if (!upgradeInASingleTransaction) {
            Jidbc.executeUpdate(dataSource, "update schema_variables set schema_version = ?", interimVersion);
        }
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            performUpgrade(jidbc, output);

            if (upgradeInASingleTransaction) {
                jidbc.executeUpdate("update schema_variables set schema_version = ?", toVersion);
            }

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
        if (!upgradeInASingleTransaction) {
            String currentSchemaVersion = SchemaManager.getSchemaVersion(dataSource);
            if (!interimVersion.equals(currentSchemaVersion)) {
                throw new RuntimeException(String.format("Unexpected error on finalization, expected schema version to be \"%s\" but it was \"%s\"", interimVersion, currentSchemaVersion));
            }
            Jidbc.executeUpdate(dataSource, "update schema_variables set schema_version = ?", toVersion);
        }
    }

    private void createSchemaVariablesTable(DataSource dataSource, PrintWriter output, String initialVersion) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {
            createSchemaVariablesTable(jidbc, output, initialVersion);
            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }
    }

    private void createSchemaVariablesTable(JidbcConnection jidbc, PrintWriter output, String initialVersion) throws IOException {
        output.printf("  - creating schema_variables table\n");
        if (databaseType == DatabaseType.sqlite) {
            jidbc.executeUpdate("create table schema_variables (\n" +
                                "   single_row_enforcer       integer   not null,\n" +
                                "   schema_version            text      not null,\n" +
                                "constraint schema_variables_pk primary key (single_row_enforcer),\n" +
                                "constraint no_more_than_one_row_chk\n" +
                                "   check (\n" +
                                "      single_row_enforcer = 42\n" +
                                "   )\n" +
                                ")");
        } else {
            jidbc.executeUpdate("create table schema_variables (\n" +
                                "   single_row_enforcer       numeric(2)     not null,\n" +
                                "   schema_version            varchar(300)   not null,\n" +
                                "constraint schema_variables_pk primary key (single_row_enforcer),\n" +
                                "constraint no_more_than_one_row_chk\n" +
                                "   check (\n" +
                                "      single_row_enforcer = 42\n" +
                                "   )\n" +
                                ")");
        }
        jidbc.executeUpdate("insert into schema_variables (single_row_enforcer,schema_version) values (42,?)", initialVersion);
    }

    protected void executeDbScript(JidbcConnection jidbc, String scriptRelativePath) {
        executeScript(jidbc, databaseType.name() + "/" + scriptRelativePath);
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

    protected void processProblemsIfAny(String[] problems) {
        if (problems.length > 0) {
            for (String problem : problems) {
                System.err.printf("\n   %s\n", WordUtils.wrap(problem, 77, "\n   ", false));
            }
            System.err.printf("\n");
            throw new RuntimeException("Problems found at end of upgrade");
        }
    }

}
