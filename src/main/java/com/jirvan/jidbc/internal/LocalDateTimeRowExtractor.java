/*
 * Copyright (c) 2019, Jirvan Pty Ltd
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

import com.jirvan.lang.SQLRuntimeException;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class LocalDateTimeRowExtractor implements RowExtractor<LocalDateTime> {

    public LocalDateTime extractRowFromResultSet(Class rowClass, RowDef rowDef, ResultSet resultSet, final boolean ignoreMissingResultSetColumns) {
        if (ignoreMissingResultSetColumns) throw new RuntimeException("ignoreMissingResultSetColumns is inappropriate for LocalDateTimeRowExtractor");
        try {

            int columnType = resultSet.getMetaData().getColumnType(1);
            if (columnType == Types.VARCHAR) {
                String stringValue = resultSet.getString(1);
                if (resultSet.wasNull()) {
                    return null;
                } else {
                    return LocalDateTime.parse(stringValue);
                }
            } else {
                Timestamp value = resultSet.getTimestamp(1);
                if (resultSet.wasNull()) {
                    return null;
                } else {
                    return value.toLocalDateTime();
                }
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

}
