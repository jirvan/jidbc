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
import com.jirvan.lang.*;
import com.jirvan.util.*;

import javax.sql.*;
import java.sql.*;
import java.util.*;

public class SchemaManager {

    public static String getSchemaVersion(DataSource dataSource) {
        try {
            return Jidbc.queryFor_String(dataSource, "select schema_version from schema_variables");
        } catch (Throwable t) {
            throw new RuntimeException(t);
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

}
