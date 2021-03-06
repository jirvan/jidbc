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

package com.jirvan.jidbc.internal;

import com.jirvan.jidbc.*;
import com.jirvan.lang.*;

import java.sql.*;

import static com.jirvan.jidbc.internal.JidbcInternalUtils.*;

public class SaveHandler extends AbstractPkWhereClauseHandler {

    public static void save(Connection connection, Object row) {
        if (rowExists(connection, row)) {
            UpdateHandler.update(connection, row);
        } else {
            InsertHandler.insert(connection, row, null);
        }
    }

    private static boolean rowExists(Connection connection, Object row) {

        // Get the table def and where clause to use
        TableDef tableDef = TableDef.getTableDefForRowClass(row.getClass());
        WhereClause whereClause = new WhereClause(tableDef, row);

        String sql = String.format("select 1 from %s\n%s",
                                   tableDef.tableName,
                                   whereClause.sql);

        try {

            // Determine if the row already exists
            PreparedStatement statement = connection.prepareStatement(sql);
            try {
                int paramIndex = 0;
                for (int i = 0; i < whereClause.parameterValues.size(); i++) {
                    setObject(statement, ++paramIndex, whereClause.parameterValues.get(i));
                }
                ResultSet resultSet = statement.executeQuery();
                try {
                    return resultSet.next();
                } finally {
                    resultSet.close();
                }
            } finally {
                statement.close();
            }

        } catch (SQLException e) {
            Jidbc.logSqlException(e, sql, whereClause.parameterValues.toArray());
            throw new SQLRuntimeException(e);
        }
    }

}
