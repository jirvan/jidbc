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
import com.jirvan.util.*;

import java.lang.annotation.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.regex.*;

public class TableDef {

    private static Map<String, TableDef> tableDefMap = new HashMap<String, TableDef>();

    Class rowClass;
    String tableName;
    String generatorSequence;
    List<ColumnDef> columnDefs = new ArrayList<ColumnDef>();
    List<ColumnDef> pkColumnDefs = new ArrayList<ColumnDef>();
    List<ColumnDef> nonPkColumnDefs = new ArrayList<ColumnDef>();

    public TableDef(Class rowClass) {
        this.rowClass = rowClass;
    }

    public void setGeneratorSequence(String generatorSequence) {
        this.generatorSequence = generatorSequence;
    }

    public static TableDef getForRowClass(Class rowClass) {
        String rowClassName = rowClass.getName();
        String zrowClassSimpleName = rowClass.getSimpleName();
        TableDef tableDef = tableDefMap.get(rowClassName);
        if (tableDef == null) {
            tableDef = extractTableDefFromRowClass(rowClass, null);
            tableDefMap.put(rowClassName, tableDef);
        }
        return tableDef;
    }

    public static void deregisterRowClasses() {
        tableDefMap = new HashMap<String, TableDef>();
    }

    public static TableDef registerRowClass(Class rowClass, String... idAttributes) {
        String rowClassName = rowClass.getName();
        TableDef tableDef = tableDefMap.get(rowClassName);
        if (tableDef != null) {
            throw new RuntimeException("A TableDef for %s already exists.  You need to ensure that the row class is registered only once and that it happens before the row class is ever used (which would trigger an automatic registration)");
        } else {
            tableDef = extractTableDefFromRowClass(rowClass, idAttributes);
            tableDefMap.put(rowClassName, tableDef);
            return tableDef;
        }
    }

    private static TableDef extractTableDefFromRowClass(final Class rowClass, final String[] idFields) {

        // Extract table stuff
        final TableDef tableDef = new TableDef(rowClass);
        String rowClassSimpleName = rowClass.getSimpleName();
        Annotation annotation = rowClass.getAnnotation(TableRow.class);
        String tableName = annotation instanceof TableRow
                           ? ((TableRow) annotation).tableName()
                           : "<Guessed>";
        if (!"<Guessed>".equals(tableName)) {
            tableDef.tableName = tableName;
        } else if (rowClassSimpleName.endsWith("Row")) {
            tableDef.tableName = guessDatabaseNameFromJavaName(rowClassSimpleName.replaceFirst("Row$", ""));
        } else {
            tableDef.tableName = guessDatabaseNameFromJavaName(rowClassSimpleName) + "s";
        }

        // Extract public field based column defs
        for (final Field field : rowClass.getFields()) {

            AttributeValueHandler.performForClass(field.getType(),
                                                  new AttributeValueHandler.ClassAction() {

                                                      public void performFor_String() {
                                                          extractAndAddColumnDefFromField(rowClass, field, tableDef, idFields);
                                                      }

                                                      public void performFor_Integer() {
                                                          extractAndAddColumnDefFromField(rowClass, field, tableDef, idFields);
                                                      }

                                                      public void performFor_Long() {
                                                          extractAndAddColumnDefFromField(rowClass, field, tableDef, idFields);
                                                      }

                                                      public void performFor_BigDecimal() {
                                                          extractAndAddColumnDefFromField(rowClass, field, tableDef, idFields);
                                                      }

                                                      public void performFor_Boolean() {
                                                          extractAndAddColumnDefFromField(rowClass, field, tableDef, idFields);
                                                      }

                                                      public void performFor_Date() {
                                                          extractAndAddColumnDefFromField(rowClass, field, tableDef, idFields);
                                                      }

                                                      public void performFor_Day() {
                                                          extractAndAddColumnDefFromField(rowClass, field, tableDef, idFields);
                                                      }

                                                  });

        }

        // Extract get/set method based column defs
        Pattern GET_METHOD_PATTERN = Pattern.compile("^get([A-Z].*)$");
        for (final Method method : rowClass.getMethods()) {
            if (method.getParameterTypes().length == 0) {

                Matcher matcher = GET_METHOD_PATTERN.matcher(method.getName());
                if (matcher.matches()) {
                    try {
                        final Method getterMethod = method;
                        final String afterGetString = matcher.group(1);
                        final String attributeName = afterGetString.substring(0, 1).toLowerCase() + afterGetString.substring(1);
                        final Method setterMethod = rowClass.getMethod("set" + afterGetString, method.getReturnType());
                        AttributeValueHandler.performForClass(getterMethod.getReturnType(),
                                                              new AttributeValueHandler.ClassAction() {

                                                                  public void performFor_String() {
                                                                      extractAndAddColumnDefFromGetterSetterMethods(rowClass, attributeName, getterMethod, setterMethod, tableDef, idFields);
                                                                  }

                                                                  public void performFor_Integer() {
                                                                      extractAndAddColumnDefFromGetterSetterMethods(rowClass, attributeName, getterMethod, setterMethod, tableDef, idFields);
                                                                  }

                                                                  public void performFor_Long() {
                                                                      extractAndAddColumnDefFromGetterSetterMethods(rowClass, attributeName, getterMethod, setterMethod, tableDef, idFields);
                                                                  }

                                                                  public void performFor_BigDecimal() {
                                                                      extractAndAddColumnDefFromGetterSetterMethods(rowClass, attributeName, getterMethod, setterMethod, tableDef, idFields);
                                                                  }

                                                                  public void performFor_Boolean() {
                                                                      extractAndAddColumnDefFromGetterSetterMethods(rowClass, attributeName, getterMethod, setterMethod, tableDef, idFields);
                                                                  }

                                                                  public void performFor_Date() {
                                                                      extractAndAddColumnDefFromGetterSetterMethods(rowClass, attributeName, getterMethod, setterMethod, tableDef, idFields);
                                                                  }

                                                                  public void performFor_Day() {
                                                                      extractAndAddColumnDefFromGetterSetterMethods(rowClass, attributeName, getterMethod, setterMethod, tableDef, idFields);
                                                                  }

                                                              });
                    } catch (NoSuchMethodException e) {
                    }
                }

            }

        }

        // Check id fields and return tableDef
        if (tableDef.pkColumnDefs.size() == 0) {
            throw new RuntimeException(String.format("Row class %s does not have any id fields (they need to be annotated with @Id or registered via TableDef.registerRowClass(Class rowClass, String... idFields)", rowClass.getName()));
        }
        if (tableDef.pkColumnDefs.size() > 1 && tableDef.generatorSequence != null) {
            throw new RuntimeException(String.format("Row class %s has more than one id field and a generatorSequence has been assigned", rowClass.getName()));
        }
        return tableDef;

    }

    private static void extractAndAddColumnDefFromField(Class rowClass, Field field, TableDef tableDef, String[] idFields) {
        ColumnDef columnDef = new ColumnDef();
        columnDef.attributeType = field.getType();
        columnDef.field = field;
        columnDef.columnName = guessDatabaseNameFromJavaName(field.getName());
        tableDef.columnDefs.add(columnDef);
        Annotation annotation = field.getAnnotation(Id.class);
        if (annotation instanceof Id) {
            if (idFields != null && idFields.length > 0) {
                throw new RuntimeException(String.format("Row class %s has annotated id fields and you have specified id fields in TableDef.registerRowClass(Class rowClass, String... idFields) (you can't do both)", rowClass.getName()));
            }
            Id idAnnotation = (Id) annotation;
            if (!"<None>".equals(idAnnotation.generatorSequence())) {
                if (tableDef.generatorSequence == null) {
                    if (columnDef.attributeType != Long.class) {
                        throw new RuntimeException(String.format("Id field %s.%s has a generatorSequence assigned but is not a Long (only type Long can be generated)", rowClass.getSimpleName(), columnDef.field.getName()));
                    }
                    tableDef.generatorSequence = idAnnotation.generatorSequence();
                } else {
                    throw new RuntimeException(String.format("Row class %s has more than one id field with a generatorSequence", rowClass.getName()));
                }
            }
            tableDef.pkColumnDefs.add(columnDef);
        } else if (idFields != null && idFields.length > 0 && Strings.in(field.getName(), idFields)) {
            tableDef.pkColumnDefs.add(columnDef);
        } else {
            tableDef.nonPkColumnDefs.add(columnDef);
        }
    }

    private static void extractAndAddColumnDefFromGetterSetterMethods(Class rowClass, String attributeName, Method getterMethod, Method setterMethod, TableDef tableDef, String[] idAttributes) {
        ColumnDef columnDef = new ColumnDef();
        columnDef.attributeType = getterMethod.getReturnType();
        columnDef.getterMethod = getterMethod;
        columnDef.setterMethod = setterMethod;
        columnDef.columnName = guessDatabaseNameFromJavaName(attributeName);
        tableDef.columnDefs.add(columnDef);
        Annotation annotation = getterMethod.getAnnotation(Id.class);
        if (annotation instanceof Id) {
            if (idAttributes != null && idAttributes.length > 0) {
                throw new RuntimeException(String.format("Row class %s has annotated id attributes and you have specified id attributes in TableDef.registerRowClass(Class rowClass, String... idAttributes) (you can't do both)", rowClass.getName()));
            }
            Id idAnnotation = (Id) annotation;
            if (!"<None>".equals(idAnnotation.generatorSequence())) {
                if (tableDef.generatorSequence == null) {
                    if (getterMethod.getReturnType() != Long.class) {
                        throw new RuntimeException(String.format("Id attribute %s.%s() has a generatorSequence assigned but is not a Long (only type Long can be generated)", rowClass.getSimpleName(), columnDef.getterMethod.getName()));
                    }
                    tableDef.generatorSequence = idAnnotation.generatorSequence();
                } else {
                    throw new RuntimeException(String.format("Row class %s has more than one id attribute with a generatorSequence", rowClass.getName()));
                }
            }
            tableDef.pkColumnDefs.add(columnDef);
        } else if (idAttributes != null && idAttributes.length > 0 && Strings.in(attributeName, idAttributes)) {
            tableDef.pkColumnDefs.add(columnDef);
        } else {
            tableDef.nonPkColumnDefs.add(columnDef);
        }
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
