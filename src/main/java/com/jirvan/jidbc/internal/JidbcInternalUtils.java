/*

Copyright (c) 2013, 2014, Jirvan Pty Ltd
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

import com.jirvan.dates.*;
import com.jirvan.lang.*;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class JidbcInternalUtils {

    public static void setObject(PreparedStatement statement, int parameterIndex, Object object) throws SQLException {
        try {
            if (object instanceof java.util.Date) {
                statement.setObject(parameterIndex, object, Types.TIMESTAMP);
            } else if (object instanceof Day) {
                statement.setObject(parameterIndex, ((Day) object).getDate(), Types.TIMESTAMP);
            } else if (object instanceof LocalDate) {
                statement.setObject(parameterIndex, ((LocalDate) object).toString(), Types.VARCHAR);
            } else if (object instanceof LocalDateTime) {
                statement.setObject(parameterIndex, ((LocalDateTime) object).toString(), Types.VARCHAR);
            } else {
                statement.setObject(parameterIndex, object);
            }
        } catch (Throwable t) {
            ParameterMetaData parameterMetaData = statement.getParameterMetaData();
            if ( parameterMetaData.getParameterCount() == 0) {
                throw new SQLRuntimeException("There are no parameter markers (? characters) in the sql but a parameter value has been given.");
            } else if ( parameterIndex >  parameterMetaData.getParameterCount()) {
                throw new SQLRuntimeException("More parameter values have been given than there are parameter markers (? characters) in the sql.");
            } else {
                throw t;
            }
        }
    }

}
