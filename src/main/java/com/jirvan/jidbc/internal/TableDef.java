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

import com.jirvan.jidbc.Id;
import com.jirvan.jidbc.TableRow;
import com.jirvan.jidbc.TableRowExtensionClass;
import com.jirvan.util.DatabaseType;
import com.jirvan.util.Strings;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

public class TableDef extends RowDef {

    String tableName;
    String generatorSequence;
    boolean ifSQLiteUseAutoincrement;
    DatabaseType[] databasesToIgnoreGeneratorSequenceFor;
    List<ColumnDef> pkColumnDefs = new ArrayList<ColumnDef>();
    List<ColumnDef> nonPkColumnDefs = new ArrayList<ColumnDef>();

    private TableDef(Class rowClass) {
        super(rowClass);
    }

    public static TableDef registerTableDefRowClass(Class rowClass, String... idAttributes) {
        String rowClassName = rowClass.getName();
        RowDef rowDef = rowDefMap.get(rowClassName);
        if (rowDef != null) {
            throw new RuntimeException("A RowDef for %s already exists.  You need to ensure that the row class is registered only once and that it happens before the row class is ever used (which would trigger an automatic registration)");
        } else {
            TableDef tableDef = extractTableDefFromRowClass(rowClass, idAttributes);
            rowDefMap.put(rowClassName, tableDef);
            return tableDef;
        }
    }

    public static TableDef getTableDefForRowClass(Class rowClass) {
        String rowClassName = rowClass.getName();
        RowDef rowDef = rowDefMap.get(rowClassName);
        if (rowDef == null || !(rowDef instanceof TableDef)) {
            rowDef = extractTableDefFromRowClass(rowClass, null);
            rowDefMap.put(rowClassName, rowDef);
        }
        return (TableDef) rowDef;
    }

    public void setGeneratorSequence(String generatorSequence) {
        this.generatorSequence = generatorSequence;
    }

    private static TableDef extractTableDefFromRowClass(final Class rowClass, final String[] idAttributes) {

        // Check for extension classes
        Class effectiveRowClass = getEffectiveRowClass(rowClass);

        // Add basic column defs
        final TableDef tableDef = new TableDef(effectiveRowClass);
        addBasicColumnDefsToRowDef(effectiveRowClass, tableDef);

        // Extract table stuff
        tableDef.tableName = getTableForEffectiveRowClass(effectiveRowClass);

        // Process annotations
        for (ColumnDef columnDef : tableDef.columnDefs) {
            if (columnDef.field != null) {
                processAnnotationsForField(effectiveRowClass, tableDef, idAttributes, columnDef);
            }
            if (columnDef.getterMethod != null) {
                processAnnotationsForGetterSetter(effectiveRowClass, tableDef, idAttributes, columnDef);
            }
        }
        for (ColumnDef columnDef : tableDef.columnDefs) {
            if (columnDef.isInPk) {
                tableDef.pkColumnDefs.add(columnDef);
            } else {
                tableDef.nonPkColumnDefs.add(columnDef);
            }
        }

        // Check id fields and return tableDef
        if (tableDef.pkColumnDefs.size() == 0) {
            throw new RuntimeException(String.format("Row class %s does not have any id fields (they need to be annotated with @Id or registered via TableDef.registerRowClass(Class rowClass, String... idFields)", effectiveRowClass.getName()));
        }
        if (tableDef.pkColumnDefs.size() > 1 && tableDef.generatorSequence != null) {
            throw new RuntimeException(String.format("Row class %s has more than one id field and a generatorSequence has been assigned", effectiveRowClass.getName()));
        }
        return tableDef;

    }

    public static String getTableForRowClass(Class rowClass) {
        return getTableForEffectiveRowClass(getEffectiveRowClass(rowClass));
    }

    private static String getTableForEffectiveRowClass(Class effectiveRowClass) {
        String rowClassSimpleName = effectiveRowClass.getSimpleName();
        Annotation tableRowAnnotation = effectiveRowClass.getAnnotation(TableRow.class);
        String annotationTableName = tableRowAnnotation instanceof TableRow
                                     ? ((TableRow) tableRowAnnotation).tableName()
                                     : "<Guessed>";
        if (!"<Guessed>".equals(annotationTableName)) {
            return annotationTableName;
        } else if (rowClassSimpleName.endsWith("Row")) {
            return guessDatabaseNameFromJavaName(rowClassSimpleName.replaceFirst("Row$", ""));
        } else {
            return guessDatabaseNameFromJavaName(rowClassSimpleName) + "s";
        }
    }

    private static Class getEffectiveRowClass(Class rowClass) {
        Class effectiveRowClass;
        Annotation tableRowExtensionClassAnnotation = rowClass.getAnnotation(TableRowExtensionClass.class);
        if (tableRowExtensionClassAnnotation != null) {
            effectiveRowClass = ((TableRowExtensionClass) tableRowExtensionClassAnnotation).baseClass();
        } else {
            effectiveRowClass = rowClass;
        }
        return effectiveRowClass;
    }

    private static void processAnnotationsForField(Class rowClass,
                                                   TableDef tableDef,
                                                   String[] idFields,
                                                   ColumnDef columnDef) {
        Annotation annotation = columnDef.field.getAnnotation(Id.class);
        if (annotation instanceof Id) {
            if (idFields != null && idFields.length > 0) {
                throw new RuntimeException(String.format("Row class %s has annotated id fields and you have specified id fields in RowDef.registerRowClass(Class rowClass, String... idFields) (you can't do both)", rowClass.getName()));
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
                tableDef.ifSQLiteUseAutoincrement = idAnnotation.ifSQLiteUseAutoincrement();
                tableDef.databasesToIgnoreGeneratorSequenceFor = idAnnotation.ignoreSequenceForDBs();
            }
            columnDef.isInPk = true;
        } else if (idFields != null && idFields.length > 0 && Strings.isIn(columnDef.field.getName(), idFields)) {
            columnDef.isInPk = true;
        }
    }

    private static void processAnnotationsForGetterSetter(Class rowClass,
                                                          TableDef tableDef,
                                                          String[] idAttributes,
                                                          ColumnDef columnDef) {
        Annotation annotation = columnDef.getterMethod.getAnnotation(Id.class);
        if (annotation instanceof Id) {
            if (idAttributes != null && idAttributes.length > 0) {
                throw new RuntimeException(String.format("Row class %s has annotated id attributes and you have specified id attributes in RowDef.registerRowClass(Class rowClass, String... idAttributes) (you can't do both)", rowClass.getName()));
            }
            Id idAnnotation = (Id) annotation;
            if (!"<None>".equals(idAnnotation.generatorSequence())) {
                if (tableDef.generatorSequence == null) {
                    if (columnDef.getterMethod.getReturnType() != Long.class) {
                        throw new RuntimeException(String.format("Id attribute %s.%s() has a generatorSequence assigned but is not a Long (only type Long can be generated)", rowClass.getSimpleName(), columnDef.getterMethod.getName()));
                    }
                    tableDef.generatorSequence = idAnnotation.generatorSequence();
                } else {
                    throw new RuntimeException(String.format("Row class %s has more than one id attribute with a generatorSequence", rowClass.getName()));
                }
                tableDef.ifSQLiteUseAutoincrement = idAnnotation.ifSQLiteUseAutoincrement();
                tableDef.databasesToIgnoreGeneratorSequenceFor = idAnnotation.ignoreSequenceForDBs();
            }
            columnDef.isInPk = true;
        } else if (idAttributes != null && idAttributes.length > 0 && Strings.isIn(columnDef.attributeName, idAttributes)) {
            columnDef.isInPk = true;
        }
    }

}
