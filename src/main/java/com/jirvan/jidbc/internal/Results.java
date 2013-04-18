/*
 * Copyright (c) 2013, Jirvan Pty Ltd
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright notice,
 *       this list of conditions and the following disclaimer in the documentation
 *       and/or other materials provided with the distribution.
 *     * Neither the name of Jirvan Pty Ltd nor the names of its contributors
 *       may be used to endorse or promote products derived from this software
 *       without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.jirvan.jidbc.internal;

import com.jirvan.lang.*;

import java.sql.*;
import java.util.*;

public class Results<T> implements Iterable<T> {

    private TableDef tableDef;
    private RowExtractor rowExtractor;
    private List<Results> connectionOpenResultses;
    private Class rowClass;
    private PreparedStatement statement;
    private ResultSet resultSet;
    private Object nextRow;

    public Results(Connection connection, List<Results> connectionOpenResultses, Class rowClass, String sql, Object... parameterValues) {

        String sqlToUse;
        if (Map.class.isAssignableFrom(rowClass)) {
            tableDef = null;
            rowExtractor = new MapRowExtractor();
            sqlToUse = sql;
        } else if (Object[].class.isAssignableFrom(rowClass)) {
            tableDef = null;
            rowExtractor = new ArrayRowExtractor();
            sqlToUse = sql;
        } else {
            tableDef = TableDef.getForRowClass(rowClass);
            rowExtractor = new ObjectRowExtractor();
            sqlToUse = sql.matches("(?si)\\s*where\\s+.*")
                       ? String.format("select * from %s %s", tableDef.tableName, sql)
                       : sql;
        }

        connectionOpenResultses.add(this);
        this.connectionOpenResultses = connectionOpenResultses;
        this.rowClass = rowClass;
        try {

            statement = connection.prepareStatement(sqlToUse);
            for (int i = 0; i < parameterValues.length; i++) {
                statement.setObject(i + 1, parameterValues[i]);
            }
            resultSet = statement.executeQuery();
            fetchNext();

        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    private void fetchNext() {
        try {
            if (resultSet.next()) {
                nextRow = rowExtractor.extractRowFromResultSet(rowClass, tableDef, resultSet);
            } else {
                close();
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public void close() {
        try {
            nextRow = null;
            connectionOpenResultses.remove(this);
            if (resultSet != null) resultSet.close();
            if (statement != null) statement.close();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public Iterator<T> iterator() {
        return new Iterator<T>() {

            public boolean hasNext() {
                return nextRow != null;
            }

            public T next() {
                T rowToReturn = (T) nextRow;
                fetchNext();
                return rowToReturn;
            }

            public void remove() {
                throw new UnsupportedOperationException("The .remove method is not supported");
            }

        };
    }

}
