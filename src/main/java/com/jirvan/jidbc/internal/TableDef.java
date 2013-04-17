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

import com.jirvan.dates.*;

import java.lang.reflect.*;
import java.math.*;
import java.util.*;

public class TableDef {

    private static Map<String, TableDef> tableDefMap = new HashMap<String, TableDef>();

    public String tableName;
    public Map<String, ColumnDef> columnDefMap = new HashMap<String, ColumnDef>();

    public static TableDef getForRowClass(Class rowClass) {
        String rowClassName = rowClass.getName();
        String rowClassSimpleName = rowClass.getSimpleName();
        TableDef tableDef = tableDefMap.get(rowClassName);
        if (tableDef == null) {
            tableDef = extractTableDefFromRowClass(rowClass, rowClassName, rowClassSimpleName);
        }
        return tableDef;
    }

    private static TableDef extractTableDefFromRowClass(Class rowClass, String rowClassName, String rowClassSimpleName) {
        final TableDef tableDef = new TableDef();
        if (rowClassSimpleName.endsWith("Row")) {
            tableDef.tableName = guessDatabaseNameFromJavaName(rowClassSimpleName.replaceFirst("Row$", ""));
        } else {
            tableDef.tableName = guessDatabaseNameFromJavaName(rowClassSimpleName) + "s";
        }
        for (final Field field : rowClass.getFields()) {

            FieldValueHandler.performWithClass(field.getType(),
                                               new FieldValueHandler.ValueAction() {

                                                   public void performWith(String value) {
                                                       extractColumnDefFromField(field, tableDef);
                                                   }

                                                   public void performWith(Integer value) {
                                                       extractColumnDefFromField(field, tableDef);
                                                   }

                                                   public void performWith(Long value) {
                                                       extractColumnDefFromField(field, tableDef);
                                                   }

                                                   public void performWith(BigDecimal value) {
                                                       extractColumnDefFromField(field, tableDef);
                                                   }

                                                   public void performWith(Boolean value) {
                                                       extractColumnDefFromField(field, tableDef);
                                                   }

                                                   public void performWith(java.util.Date value) {
                                                       extractColumnDefFromField(field, tableDef);
                                                   }

                                                   public void performWith(Day value) {
                                                       extractColumnDefFromField(field, tableDef);
                                                   }

                                               });

        }
        tableDefMap.put(rowClassName, tableDef);
        return tableDef;
    }

    private static void extractColumnDefFromField(Field field, TableDef tableDef) {
        ColumnDef columnDef = new ColumnDef();
        columnDef.field = field;
        columnDef.columnName = guessDatabaseNameFromJavaName(field.getName());
        tableDef.columnDefMap.put(field.getName(), columnDef);
    }

    private static String guessDatabaseNameFromJavaName(String fieldName) {
        StringBuffer buf = new StringBuffer();
        char[] chars = fieldName.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];
            if (i == 0) {
                if (c < 'A' || c > 'Z') {
                    buf.append(c);
                } else {
                    buf.append((char) (c - ('A' - 'a')));
                }
            } else {
                if ('0' <= c && c <= '9') {
                    char previousChar = chars[i - 1];
                    if ('a' <= previousChar && previousChar <= 'z') {
                        buf.append('_');
                    }
                    buf.append(c);
                } else if (c < 'A' || c > 'Z') {
                    buf.append(c);
                } else {
                    buf.append('_');
                    buf.append((char) (c - ('A' - 'a')));
                }
            }
        }
        return buf.toString();
    }

}
