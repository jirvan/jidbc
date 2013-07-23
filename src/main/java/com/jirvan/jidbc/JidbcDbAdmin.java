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

import javax.sql.*;

public class JidbcDbAdmin {

    public static void dropAllCurrentUsersTablesViewsAndSequences(DataSource dataSource) {
        String databaseProductName = Jidbc.getDatabaseProductName(dataSource);
        if ("PostgreSQL".equals(databaseProductName)) {
            dropAllCurrentUsersTablesViewsAndSequences_postgres(dataSource);
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

    private static void dropAllCurrentUsersTablesViewsAndSequences_postgres(DataSource dataSource) {
        JidbcConnection jidbc = JidbcConnection.from(dataSource);
        try {

            // Drop tables
            for (String table : jidbc.<String>queryForList(String.class, "select relname\n" +
                                                                         "from pg_class\n" +
                                                                         "     join pg_user on pg_user.usesysid = pg_class.relowner\n" +
                                                                         "where pg_user.usename = current_user\n" +
                                                                         "  and pg_class.relkind = 'r'")) {
                jidbc.executeUpdate(String.format("drop table %s cascade", table));
            }

            // Drop views
            for (String view : jidbc.<String>queryForList(String.class, "select relname\n" +
                                                                        "from pg_class\n" +
                                                                        "     join pg_user on pg_user.usesysid = pg_class.relowner\n" +
                                                                        "where pg_user.usename = current_user\n" +
                                                                        "  and pg_class.relkind = 'v'")) {
                jidbc.executeUpdate(String.format("drop view %s cascade", view));
            }

            // Drop sequences
            for (String sequence : jidbc.<String>queryForList(String.class, "select relname\n" +
                                                                            "from pg_class\n" +
                                                                            "     join pg_user on pg_user.usesysid = pg_class.relowner\n" +
                                                                            "where pg_user.usename = current_user\n" +
                                                                            "  and pg_class.relkind = 'S'")) {
                jidbc.executeUpdate(String.format("drop sequence %s cascade", sequence));
            }

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

    }

}
