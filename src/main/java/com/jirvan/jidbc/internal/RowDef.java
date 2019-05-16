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

import com.jirvan.jidbc.JidbcIgnore;
import com.jirvan.jidbc.StoreAsTimestamp;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RowDef {

    protected static Map<String, RowDef> rowDefMap = new HashMap<String, RowDef>();

    Class rowClass;
    List<ColumnDef> columnDefs = new ArrayList<>();

    protected RowDef(Class rowClass) {
        this.rowClass = rowClass;
    }

    public static RowDef getRowDefForRowClass(Class rowClass) {
        String rowClassName = rowClass.getName();
        RowDef rowDef = rowDefMap.get(rowClassName);
        if (rowDef == null) {
            rowDef = extractRowDefFromRowClass(rowClass, null);
            rowDefMap.put(rowClassName, rowDef);
        }
        return rowDef;
    }

    public static void deregisterRowClasses() {
        rowDefMap = new HashMap<String, RowDef>();
    }

    private static RowDef extractRowDefFromRowClass(final Class rowClass, final String[] idAttributes) {

        RowDef rowDef = new RowDef(rowClass);
        addBasicColumnDefsToRowDef(rowClass, rowDef);
        return rowDef;

    }

    protected static void addBasicColumnDefsToRowDef(Class rowClass, final RowDef rowDef) {

        // Extract public field based column defs
        for (final Field field : rowClass.getFields()) {
            Annotation annotation = field.getAnnotation(JidbcIgnore.class);
            if (!(annotation instanceof JidbcIgnore) && !Collection.class.isAssignableFrom(field.getType())) {

                AttributeValueHandler.performForClass(field.getType(),
                                                      new AttributeValueHandler.ClassAction() {

                                                          public void performFor_String() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_Integer() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_Long() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_BigDecimal() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_Boolean() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_Date() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_LocalDate() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_LocalDateTime() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_ZonedDateTime() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_Month() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_Day() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_Hour() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_Minute() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_Second() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_Millisecond() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_Enum(Class fieldClass) {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }

                                                          public void performFor_byteArray() {
                                                              extractAndAddColumnDefFromField(field, rowDef);
                                                          }
                                                      });
            }
        }

        // Extract get/set method based column defs
        Pattern GET_METHOD_PATTERN = Pattern.compile("^get([A-Z].*)$");
        for (final Method method : rowClass.getMethods()) {
            if (method.getParameterTypes().length == 0) {

                Annotation annotation = method.getAnnotation(JidbcIgnore.class);
                if (!(annotation instanceof JidbcIgnore) && !Collection.class.isAssignableFrom(method.getReturnType())) {

                    Matcher matcher = GET_METHOD_PATTERN.matcher(method.getName());
                    if (matcher.matches()) {
                        try {
                            final Method getterMethod = method;
                            final String afterGetString = matcher.group(1);
                            final String attributeName = afterGetString.substring(0, 1).toLowerCase() + afterGetString.substring(1);
                            ColumnDef columnDef = rowDef.columnDefForAttribute(attributeName);
                            if (columnDef != null) {
                                columnDef.getterMethod = getterMethod;
                            } else {
                                final Method setterMethod = rowClass.getMethod("set" + afterGetString, method.getReturnType());
                                AttributeValueHandler.performForClass(getterMethod.getReturnType(),
                                                                      new AttributeValueHandler.ClassAction() {

                                                                          public void performFor_String() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_Integer() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_Long() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_BigDecimal() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_Boolean() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_Date() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_LocalDate() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_LocalDateTime() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_ZonedDateTime() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_Month() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_Day() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_Hour() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_Minute() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_Second() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_Millisecond() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_Enum(Class fieldClass) {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }

                                                                          public void performFor_byteArray() {
                                                                              extractAndAddColumnDefFromGetterSetterMethods(attributeName, getterMethod, setterMethod, rowDef);
                                                                          }
                                                                      });
                            }
                        } catch (NoSuchMethodException e) {
                        }
                    }
                }

            }

        }
    }

    private ColumnDef columnDefForAttribute(String attributeName) {
        for (ColumnDef columnDef : columnDefs) {
            if (columnDef.attributeName.equals(attributeName)) {
                return columnDef;
            }
        }
        return null;
    }

    private static void extractAndAddColumnDefFromField(Field field, RowDef rowDef) {
        ColumnDef columnDef = new ColumnDef();
        columnDef.attributeName = field.getName();
        columnDef.attributeType = field.getType();
        columnDef.storeAsTimestamp = field.getAnnotation(StoreAsTimestamp.class) != null;
        columnDef.field = field;
        columnDef.columnName = guessDatabaseNameFromJavaName(field.getName());
        rowDef.columnDefs.add(columnDef);
    }

    private static void extractAndAddColumnDefFromGetterSetterMethods(String attributeName, Method getterMethod, Method setterMethod, RowDef rowDef) {
        ColumnDef columnDef = new ColumnDef();
        columnDef.attributeName = attributeName;
        columnDef.attributeType = getterMethod.getReturnType();
        columnDef.storeAsTimestamp = getterMethod.getAnnotation(StoreAsTimestamp.class) != null;
        columnDef.getterMethod = getterMethod;
        columnDef.setterMethod = setterMethod;
        columnDef.columnName = guessDatabaseNameFromJavaName(attributeName);
        rowDef.columnDefs.add(columnDef);
    }

    protected static String guessDatabaseNameFromJavaName(String fieldName) {
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
