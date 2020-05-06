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

package com.jirvan.jidbc;

import com.jirvan.lang.MessageException;
import com.jirvan.util.Strings;

import javax.sql.DataSource;
import java.util.List;

public class JidbcDbAdmin {

    public static String getCurrentUser(DataSource dataSource) {
        String databaseProductName = Jidbc.getDatabaseProductName(dataSource);
        if ("PostgreSQL".equals(databaseProductName)) {
            return getCurrentUser_postgres(dataSource);
        } else if ("Microsoft SQL Server".equals(databaseProductName)) {
            return getCurrentUser_sqlserver(dataSource);
        } else {
            throw new RuntimeException(String.format("%s is not supported (PostgreSQL and SQLServer are the only databases currently supported by getCurrentUser)", databaseProductName));
        }
    }

    public static String[] getCurrentUsersTables(DataSource dataSource) {
        String databaseProductName = Jidbc.getDatabaseProductName(dataSource);
        if ("PostgreSQL".equals(databaseProductName)) {
            return getCurrentUsersRelation_postgres(dataSource, "r");
        } else if ("SQLite".equals(databaseProductName)) {
            return getCurrentUsersTables_sqlite(dataSource);
        } else if ("Microsoft SQL Server".equals(databaseProductName)) {
            if (Jidbc.getDatabaseMajorVersion(dataSource) >= 10) {
                return getCurrentUsersTables_sqlserver(dataSource);
            } else {
                throw new RuntimeException(String.format("This version of %s is not supported (PostgreSQL, SQLite and SQLServer 2008 are the only databases currently supported by getCurrentUsersTables)", databaseProductName));
            }
        } else {
            throw new RuntimeException(String.format("%s is not supported (PostgreSQL, SQLite and SQLServer 2008 are the only databases currently supported by getCurrentUsersTables)", databaseProductName));
        }
    }

    public static String[] getCurrentUsersViews(DataSource dataSource) {
        String databaseProductName = Jidbc.getDatabaseProductName(dataSource);
        if ("PostgreSQL".equals(databaseProductName)) {
            return getCurrentUsersRelation_postgres(dataSource, "v");
        } else if ("SQLite".equals(databaseProductName)) {
            return getCurrentUsersViews_sqlite(dataSource);
        } else if ("Microsoft SQL Server".equals(databaseProductName)) {
            if (Jidbc.getDatabaseMajorVersion(dataSource) == 10) {
                return getCurrentUsersViews_sqlserver(dataSource);
            } else {
                throw new RuntimeException(String.format("This version of %s is not supported (PostgreSQL, SQLite and SQLServer 2008 are the only databases currently supported by getCurrentUsersViews)", databaseProductName));
            }
        } else {
            throw new RuntimeException(String.format("%s is not supported (PostgreSQL, SQLite and SQLServer 2008 are the only databases currently supported by getCurrentUsersViews)", databaseProductName));
        }
    }

    public static String[] getCurrentUsersSequences(DataSource dataSource) {
        String databaseProductName = Jidbc.getDatabaseProductName(dataSource);
        if ("PostgreSQL".equals(databaseProductName)) {
            return getCurrentUsersRelation_postgres(dataSource, "S");
        } else if ("SQLite".equals(databaseProductName)) {
            return new String[0];
        } else if ("Microsoft SQL Server".equals(databaseProductName)) {
            if (Jidbc.getDatabaseMajorVersion(dataSource) == 10) {
                return new String[0]; // SQLServer 2008 does not support sequences
            } else {
                throw new RuntimeException(String.format("This version of %s is not supported (PostgreSQL, SQLite and SQLServer 2008 are the only databases currently supported by getCurrentUsersSequences)", databaseProductName));
            }
        } else {
            throw new RuntimeException(String.format("%s is not supported (PostgreSQL, SQLite and SQLServer 2008 are the only databases currently supported by getCurrentUsersSequences)", databaseProductName));
        }
    }

    public static void verifyNoTablesViewsOrSequencesExistOwnedByCurrentUser(DataSource dataSource) {
        String databaseProductName = Jidbc.getDatabaseProductName(dataSource);
        String userOrDbText;
        if ("PostgreSQL".equals(databaseProductName)) {
            userOrDbText = "User " + JidbcDbAdmin.getCurrentUser(dataSource);
        } else if ("SQLite".equals(databaseProductName)) {
            userOrDbText = "Database ";
        } else if ("Microsoft SQL Server".equals(databaseProductName)) {
            userOrDbText = "User " + JidbcDbAdmin.getCurrentUser(dataSource);
        } else {
            throw new RuntimeException(String.format("%s is not supported (PostgreSQL, SQLite and SQLServer 2008 are the only databases currently supported by getCurrentUsersSequences)", databaseProductName));
        }

        // Check user has no existing tables etc
        String[] existingTables = JidbcDbAdmin.getCurrentUsersTables(dataSource);
        if (existingTables.length > 0) {
            throw new MessageException(String.format("%s currently has some tables.  The tables are: %s.",
                                                     userOrDbText,
                                                     Strings.join(existingTables, ',')));
        }
        String[] existingViews = JidbcDbAdmin.getCurrentUsersViews(dataSource);
        if (existingViews.length > 0) {
            throw new MessageException(String.format("%s currently has some views.  The views are: %s.",
                                                     userOrDbText,
                                                     Strings.join(existingViews, ',')));
        }
        String[] existingSequences = JidbcDbAdmin.getCurrentUsersSequences(dataSource);
        if (existingSequences.length > 0) {
            throw new MessageException(String.format("%s currently has some sequences.  The sequences are: %s.",
                                                     userOrDbText,
                                                     Strings.join(existingSequences, ',')));
        }
    }

    public static DBObjects dropAllCurrentUsersTablesViewsAndSequences(DataSource dataSource) {
        String databaseProductName = Jidbc.getDatabaseProductName(dataSource);
        if ("PostgreSQL".equals(databaseProductName)) {
            return dropAllCurrentUsersTablesViewsAndSequences_postgres(dataSource);
        } else {
            throw new RuntimeException(String.format("%s is not supported (PostgreSQL is the only database currently supported by dropAllCurrentUsersTablesViewsAndSequences)", databaseProductName));
        }
    }

    public static void forciblyDisconnectConnectionsToDatabase(DataSource dataSource, String database) {

        // Get database type info etc
        String databaseProductName;
        int databaseMajorVersion;
        int databaseMinorVersion;
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {
            databaseProductName = jidbc.getDatabaseProductName();
            databaseMajorVersion = jidbc.getDatabaseMajorVersion();
            databaseMinorVersion = jidbc.getDatabaseMinorVersion();
            jidbc.rollbackAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }


        if ("PostgreSQL".equals(databaseProductName)) {
            forciblyDisconnectConnectionsToPostgresDatabase(dataSource, databaseMajorVersion, databaseMinorVersion, database);
        } else {
            throw new RuntimeException(String.format("%s is not supported (PostgreSQL is the only database currently supported by startRecreateMainDatabase)", databaseProductName));
        }

    }

    private static void forciblyDisconnectConnectionsToPostgresDatabase(DataSource dataSource,
                                                                        int databaseMajorVersion,
                                                                        int databaseMinorVersion,
                                                                        String database) {

        String pidColumnName;
        if (databaseMajorVersion < 9) {
            throw new RuntimeException(String.format("PostgreSQL %d is not supported (only PostgreSQL version 9 and up are currently supported)", databaseMajorVersion));
        } else if (databaseMajorVersion == 9 && databaseMinorVersion < 2) {
            pidColumnName = "procpid";
        } else {
            pidColumnName = "pid";
        }

        Jidbc.executeStatement(dataSource, String.format("update pg_database set datallowconn = 'false' where datname = '%s'", database));
        Jidbc.executeStatement(dataSource, String.format("select pg_terminate_backend(%s) from pg_stat_activity where datname = '%s'",
                                                         pidColumnName, database));

    }

    public static boolean databaseExists(DataSource dataSource, String database) {
        String databaseProductName = Jidbc.getDatabaseProductName(dataSource);
        if ("PostgreSQL".equals(databaseProductName)) {
            return Jidbc.queryFor_Integer(dataSource, "select count(*) from pg_database where datname = ?", database) > 0;
        } else {
            throw new RuntimeException(String.format("%s is not supported (PostgreSQL is the only database currently supported by databaseExists)", databaseProductName));
        }
    }

    private static DBObjects dropAllCurrentUsersTablesViewsAndSequences_postgres(DataSource dataSource) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            // Get the things to dop
            DBObjects thingsToDrop = new DBObjects();
            thingsToDrop.tables = jidbc.queryForList(String.class, "select relname\n" +
                                                                   "from pg_class\n" +
                                                                   "     join pg_user on pg_user.usesysid = pg_class.relowner\n" +
                                                                   "where pg_user.usename = current_user\n" +
                                                                   "  and pg_class.relkind = 'r'");
            thingsToDrop.views = jidbc.queryForList(String.class, "select relname\n" +
                                                                  "from pg_class\n" +
                                                                  "     join pg_user on pg_user.usesysid = pg_class.relowner\n" +
                                                                  "where pg_user.usename = current_user\n" +
                                                                  "  and pg_class.relkind = 'v'");
            thingsToDrop.sequences = jidbc.<String>queryForList(String.class, "select relname\n" +
                                                                              "from pg_class\n" +
                                                                              "     join pg_user on pg_user.usesysid = pg_class.relowner\n" +
                                                                              "where pg_user.usename = current_user\n" +
                                                                              "  and pg_class.relkind = 'S'");

            // Drop everything
            for (String table : thingsToDrop.tables) {
                jidbc.executeUpdate(String.format("drop table %s cascade", table));
            }
            for (String view : thingsToDrop.views) {
                jidbc.executeUpdate(String.format("drop view if exists %s cascade", view));
            }
            for (String sequence : thingsToDrop.sequences) {
                jidbc.executeUpdate(String.format("drop sequence if exists %s cascade", sequence));
            }

            jidbc.commitAndClose();
            return thingsToDrop;
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

    }

    public static class DBObjects {
        public List<String> tables;
        public List<String> views;
        public List<String> sequences;
    }

    private static String getCurrentUser_postgres(DataSource dataSource) {
        return Jidbc.queryFor_String(dataSource, "select current_user");
    }

    private static String getCurrentUser_sqlserver(DataSource dataSource) {
        return Jidbc.queryFor_String(dataSource, "select system_user");
    }

    public static String[] getCurrentUsersTables_sqlserver(DataSource dataSource) {
        return Jidbc.<String>queryForList(dataSource, String.class, "select table_name\n" +
                                                                    "from information_schema.tables\n" +
                                                                    "where table_type = 'BASE TABLE'")
                    .toArray(new String[0]);
    }

    public static String[] getCurrentUsersTables_sqlite(DataSource dataSource) {
        return Jidbc.<String>queryForList(dataSource, String.class, "select name\n" +
                                                                    "from sqlite_master\n" +
                                                                    "where type = 'table'")
                    .toArray(new String[0]);
    }

    public static String[] getCurrentUsersViews_sqlite(DataSource dataSource) {
        return Jidbc.<String>queryForList(dataSource, String.class, "select name\n" +
                                                                    "from sqlite_master\n" +
                                                                    "where type = 'view'")
                    .toArray(new String[0]);
    }

    public static String[] getCurrentUsersViews_sqlserver(DataSource dataSource) {
        return Jidbc.<String>queryForList(dataSource, String.class, "select table_name\n" +
                                                                    "from information_schema.tables\n" +
                                                                    "where table_type = 'VIEW'")
                    .toArray(new String[0]);
    }

    public static String[] getCurrentUsersRelation_postgres(DataSource dataSource, String relkind) {
        return Jidbc.<String>queryForList(dataSource, String.class, "select relname\n" +
                                                                    "from pg_class\n" +
                                                                    "     join pg_user on pg_user.usesysid = pg_class.relowner\n" +
                                                                    "where pg_user.usename = current_user\n" +
                                                                    "  and pg_class.relkind = ?", relkind)
                    .toArray(new String[0]);
    }

}
