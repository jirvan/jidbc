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

public class CheckConstraint {

    public String schemaName;
    public String tableName;
    public String checkConstraintName;
    public String checkConstraintText;

    public static CheckConstraint[] getCheckConstraintsForTable(Connection connection, DatabaseMetaData databaseMetaData, Table table) throws SQLException {
        if (databaseMetaData == null) throw new RuntimeException("databaseMetaData cannot be null");
        if (table == null) throw new RuntimeException("table cannot be null");

        String databaseProductName = databaseMetaData.getDatabaseProductName();
        if ("PostgreSQL".equals(databaseProductName)) {
            return getCheckConstraintsForTable_PostgreSQL(connection, table);
        } else {
            System.out.printf("Cannot generate check constraints for %s databases.", databaseProductName);
            return new CheckConstraint[0];
//            throw new RuntimeException(String.format("Cannot generate check constraints for %s databases.", databaseProductName));
        }
    }

    public static CheckConstraint[] getCheckConstraintsForTable_PostgreSQL(Connection connection, Table table) throws SQLException {
        PreparedStatement stmt = connection.prepareStatement("select constraint_name,\n" +
                                                             "       check_clause\n" +
                                                             "from information_schema.table_constraints\n" +
                                                             "     join information_schema.check_constraints using (constraint_catalog, constraint_schema, constraint_name)\n" +
                                                             "where constraint_type = 'CHECK'\n" +
                                                             "  and table_schema = ?\n" +
                                                             "  and table_name   = ?");
        try {
            stmt.setString(1, table.schemaName);
            stmt.setString(2, table.tableName);
            ResultSet rset = stmt.executeQuery();
            try {
                Vector<CheckConstraint> vector = new Vector<CheckConstraint>();
                while (rset.next()) {
                    String constraintName = rset.getString(1);
                    String checkClause = rset.getString(2);
                    //if (!checkClause.matches("(?i)^\\B+ is not null$")) {
                    if (!checkClause.matches("^[a-zA-Z0-9_]+ IS NOT NULL$")) {
                        CheckConstraint checkConstraint = new CheckConstraint();
                        checkConstraint.checkConstraintName = constraintName;
                        checkConstraint.checkConstraintText = checkClause;
                        vector.add(checkConstraint);
                    }
                }
                return vector.toArray(new CheckConstraint[vector.size()]);
            } finally {
                rset.close();
            }
        } finally {
            stmt.close();
        }

    }

}