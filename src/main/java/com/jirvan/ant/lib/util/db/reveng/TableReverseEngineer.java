/*

Copyright (c) 2008,2009 Jirvan Pty Ltd
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

import com.jirvan.util.*;

import java.io.*;
import java.sql.*;

public class TableReverseEngineer {

    public static void reverseEngineer(File tablesDir, Table table, CaseSetter nameCase, boolean lowerCaseDataTypes) throws IOException {
        String fileName = "CrTab_" + nameCase.set(table.tableName) + ".sql";
        PrintWriter writer = new PrintWriter(new File(tablesDir, fileName));
        try {
            reverseEngineer(writer, table, nameCase, lowerCaseDataTypes);
        } finally {
            writer.close();
        }

    }

    public static void reverseEngineer(PrintWriter writer, Table table, CaseSetter nameCase, boolean lowerCaseDataTypes) {

        int maxDataDefinitionWidth = maxDataDefinitionWidth(table, lowerCaseDataTypes);

        // Write create clause
        writer.printf("create table %s (", nameCase.set(table.tableName));

        // Write column definitions
        for (int i = 0; i < table.columns.length; i++) {
            Column column = table.columns[i];
            if (i != 0) writer.printf(",");
            if (column.notNull) {
                writer.printf("\n  %-" + table.maxColumnNameLength + "s  %-" + maxDataDefinitionWidth + "s   not null", nameCase.set(column.columnName), formatDataDefinition(column, lowerCaseDataTypes));
            } else {
                writer.printf("\n  %-" + table.maxColumnNameLength + "s  %s", nameCase.set(column.columnName), formatDataDefinition(column, lowerCaseDataTypes));
            }
        }

        // Write Primary Key (if any)
        if (table.primaryKey != null) {
            writer.printf(",\nconstraint %s primary key (", table.primaryKey.primaryKeyName);
            for (int i = 0; i < table.primaryKey.columnNames.length; i++) {
                if (i != 0) {
                    writer.printf(",\n           %" + table.primaryKey.primaryKeyName.length() + "s              ", "");
                }
                writer.print(nameCase.set(table.primaryKey.columnNames[i]));
            }
            writer.print(")");
        }

        // Write Unique Keys (if any)
        if (table.uniqueKeys != null) {
            for (UniqueKey uniqueKey : table.uniqueKeys) {
                writer.printf(",\nconstraint %s unique (", uniqueKey.uniqueKeyName);
                for (int i = 0; i < uniqueKey.columnNames.length; i++) {
                    if (i != 0) {
                        writer.printf(",\n           %" + uniqueKey.uniqueKeyName.length() + "s         ", "");
                    }
                    writer.print(nameCase.set(uniqueKey.columnNames[i]));
                }
                writer.print(")");
            }
        }

        // Write Check Constraints (if any)
        if (table.checkConstraints != null) {
            for (CheckConstraint checkConstraint : table.checkConstraints) {
                writer.printf(",\nconstraint %s\n", checkConstraint.checkConstraintName);
                writer.print("   check (\n");
                writer.printf("      %s\n", checkConstraint.checkConstraintText);
                writer.print("   )");
            }
        }

//            writer.printf(",\n  current_upload_status    varchar(100)      not null");
//            writer.printf(",\n  single_row_lock_column   integer default 1 not null");


        writer.printf("\n);");

        if (table.nonUniqueIndexes != null && table.nonUniqueIndexes.length > 0) {
            writer.printf("\n");
            for (NonUniqueIndex nonUniqueIndex : table.nonUniqueIndexes) {
                writer.printf("\ncreate index %s on %s (", nonUniqueIndex.indexName, nameCase.set(table.tableName));
                for (int i = 0; i < nonUniqueIndex.columnNames.length; i++) {
                    if (i != 0) {
                        writer.printf(",\n             %" + nonUniqueIndex.indexName.length() + "s    %" + table.tableName.length() + "s (", "", "");
                    }
                    writer.print(nameCase.set(nonUniqueIndex.columnNames[i]));
                }
                writer.print(");");
            }
        }
    }

    public static void reverseEngineerAsTable(File tablesDir, View view, CaseSetter nameCase, boolean lowerCaseDataTypes) throws SQLException, IOException {

        String fileName = "CrTab_" + nameCase.set(view.viewName) + ".sql";
        PrintWriter writer = new PrintWriter(new File(tablesDir, fileName));
        try {

            reverseEngineerAsTable(writer, view, nameCase, lowerCaseDataTypes);

        } finally {
            writer.close();
        }
    }

    public static void reverseEngineerAsTable(PrintWriter writer, View view, CaseSetter nameCase, boolean lowerCaseDataTypes) {

        int maxDataDefinitionWidth = maxDataDefinitionWidth(view, lowerCaseDataTypes);

        // Write create clause
        writer.printf("create table %s (", nameCase.set(view.viewName));

        // Write column definitions
        for (int i = 0; i < view.columns.length; i++) {
            Column column = view.columns[i];
            if (i != 0) writer.printf(",");
            if (column.notNull) {
                writer.printf("\n  %-" + view.maxColumnNameLength + "s  %-" + maxDataDefinitionWidth + "s   not null", nameCase.set(column.columnName), formatDataDefinition(column, lowerCaseDataTypes));
            } else {
                writer.printf("\n  %-" + view.maxColumnNameLength + "s  %s", nameCase.set(column.columnName), formatDataDefinition(column, lowerCaseDataTypes));
            }
        }


        writer.printf("\n);");
    }

    public static void reverseEngineerForeignKeys(File tablesDir, Table table, CaseSetter nameCase) throws SQLException, IOException {

        String fileName = "CrFKeysFor_" + nameCase.set(table.tableName) + ".sql";
        PrintWriter writer = new PrintWriter(new File(tablesDir, fileName));
        try {
            reverseEngineerForeignKeys(writer, table, nameCase);
        } finally {
            writer.close();
        }
    }

    public static void reverseEngineerForeignKeys(PrintWriter writer, Table table, CaseSetter nameCase) {
        for (int fkIndex = 0; fkIndex < table.foreignKeys.length; fkIndex++) {
            ForeignKey foreignKey = table.foreignKeys[fkIndex];
            if (fkIndex != 0) writer.print("\n\n");
            writer.printf("alter table %s add\n", nameCase.set(table.tableName));
            writer.printf(" constraint %s foreign key (", foreignKey.foreignKeyName);
            for (int i = 0; i < foreignKey.columnNamePairs.length; i++) {
                if (i != 0) writer.printf(",\n            %" + foreignKey.foreignKeyName.length() + "s              ", "");
                writer.print(nameCase.set(foreignKey.columnNamePairs[i].columnName));
            }
            writer.print(")\n");
            writer.printf(" references %-" + (foreignKey.foreignKeyName + " foreign key ").length() + "s(", nameCase.set(foreignKey.referencedTableName));
            for (int i = 0; i < foreignKey.columnNamePairs.length; i++) {
                if (i != 0) writer.printf(",\n            %" + foreignKey.foreignKeyName.length() + "s              ", "");
                writer.print(nameCase.set(foreignKey.columnNamePairs[i].referencedColumnName));
            }
            writer.print(");");
        }
    }

    private static int maxDataDefinitionWidth(Table table, boolean lowerCaseDataTypes) {
        int max = 0;
        for (Column column : table.columns) {
            max = Math.max(max, formatDataDefinition(column, lowerCaseDataTypes).length());
        }
        return max;
    }

    private static int maxDataDefinitionWidth(View view, boolean lowerCaseDataTypes) {
        int max = 0;
        for (Column column : view.columns) {
            max = Math.max(max, formatDataDefinition(column, lowerCaseDataTypes).length());
        }
        return max;
    }

    public static String formatDataDefinition(Column column, boolean lowerCaseDataTypes) {
        String dataType = column.sourceDataType != null && lowerCaseDataTypes ? column.sourceDataType.toLowerCase() : column.sourceDataType;
        if (Strings.inIgnoreCase(dataType, new String[]{"integer",
                                                        "double",
                                                        "float",
                                                        "date",
                                                        "time",
                                                        "timestamp"})) {
            if (column.defaultValue != null) {
                return String.format("%s default %s", dataType, column.defaultValue);
            } else {
                return String.format("%s", dataType);
            }
        } else {
            if (column.decimalDigits == null) {
                if (column.defaultValue != null) {
                    if (column.columnSize != null) {
                        return String.format("%s(%s) default %s", dataType, column.columnSize.toString(), column.defaultValue);
                    } else {
                        return String.format("%s default %s", dataType, column.defaultValue);
                    }
                } else {
                    if (column.columnSize != null) {
                        return String.format("%s(%s)", dataType, column.columnSize.toString());
                    } else {
                        return String.format("%s", dataType);
                    }
                }
            } else {
                if (column.defaultValue != null) {
                    return String.format("%s(%s,%s) default %s", dataType, column.columnSize.toString(), column.decimalDigits.toString(), column.defaultValue);
                } else {
                    return String.format("%s(%s,%s)", dataType, column.columnSize.toString(), column.decimalDigits.toString());
                }
            }
        }
    }

}