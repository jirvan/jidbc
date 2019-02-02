/*

Copyright (c) 2013,2014 Jirvan Pty Ltd
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

package com.jirvan.jidbc;

import com.jirvan.lang.SQLRuntimeException;
import com.jirvan.util.Io;
import com.jirvan.util.Strings;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.jirvan.util.Assertions.*;

public class JidbcRowClassGenerator {

    public static void generateJavaFilesIntoDirectory(DataSource dataSource, String packageName, boolean includeViews, File outputDirectory) {
        generateJavaFilesIntoDirectory(dataSource, packageName, null, "Row", "Vrow", includeViews, false, outputDirectory, false);
    }

    public static void generateJavaFilesIntoDirectory(DataSource dataSource, String packageName, boolean includeViews, File outputDirectory, boolean replaceExistingFiles) {
        generateJavaFilesIntoDirectory(dataSource, packageName, null, "Row", "Vrow", includeViews, false, outputDirectory, replaceExistingFiles);
    }

    public static void generateJavaFilesIntoDirectory(DataSource dataSource,
                                                      String packageName,
                                                      String classNamePrefix,
                                                      String tableClassNameSuffix,
                                                      String viewClassNameSuffix,
                                                      boolean includeViews,
                                                      boolean includeCloneMethod,
                                                      File outputDirectory) {
        generateJavaFilesIntoDirectory(dataSource,
                                       packageName,
                                       classNamePrefix,
                                       tableClassNameSuffix,
                                       viewClassNameSuffix,
                                       includeViews,
                                       includeCloneMethod,
                                       outputDirectory,
                                       false);
    }

    public static void generateJavaFilesIntoDirectory(DataSource dataSource,
                                                      String packageName,
                                                      String classNamePrefix,
                                                      String tableClassNameSuffix,
                                                      String viewClassNameSuffix,
                                                      boolean includeViews,
                                                      boolean includeCloneMethod,
                                                      File outputDirectory,
                                                      boolean replaceExistingFiles) {
        try {
            Io.ensureDirectoryExists(outputDirectory);
            Connection connection = dataSource.getConnection();
            try {
                ResultSet resultSet = connection.getMetaData().getTables(null,
                                                                         null,
                                                                         null,
                                                                         includeViews ? new String[]{"TABLE", "VIEW"}
                                                                                      : new String[]{"TABLE"});
                try {

                    while (resultSet.next()) {
                        String tableName = resultSet.getString("TABLE_NAME");
                        String tableType = resultSet.getString("TABLE_TYPE");
                        String classNameSuffix;
                        boolean isView;
                        if ("TABLE".equals(tableType)) {
                            classNameSuffix = tableClassNameSuffix;
                            isView = false;
                        } else if ("VIEW".equals(tableType)) {
                            classNameSuffix = viewClassNameSuffix;
                            isView = true;
                        } else {
                            throw new RuntimeException(String.format("Unexpected table (relation) type \"%s\"", tableType));
                        }
                        String classSimpleName = (classNamePrefix == null ? "" : classNamePrefix)
                                                 + toCamelHumpName(tableName, true)
                                                 + (classNameSuffix == null ? "" : classNameSuffix);
                        String tableCatalog = resultSet.getString("TABLE_CAT");
                        String tableSchema = resultSet.getString("TABLE_SCHEM");
                        SortedSet<String> imports = new TreeSet<String>();
                        List<String> pkColumnNames = getPkColumnNames(connection, tableCatalog, tableSchema, tableName);
                        List<ColumnDetails> columnDetailses = getColumnDetailses(connection, tableSchema, tableName, pkColumnNames, imports);
                        try {
                            File outputJavaFile = new File(outputDirectory, classSimpleName + ".java");
                            if (!replaceExistingFiles) {
                                assertFileDoesNotExist(outputJavaFile);
                            }
                            PrintStream printStream = new PrintStream(outputJavaFile);
                            try {
                                if (classNamePrefix != null || (classNameSuffix != null && !classNameSuffix.equals("Row"))) {
                                    generateJavaFile(printStream, packageName, imports, columnDetailses, classSimpleName, tableName, isView, includeCloneMethod);
                                } else {
                                    generateJavaFile(printStream, packageName, imports, columnDetailses, classSimpleName, null, isView, includeCloneMethod);
                                }
                            } finally {
                                printStream.close();
                            }
                        } catch (FileNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                    }

                } finally {
                    resultSet.close();
                }
            } finally {
                connection.close();
            }

        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public static void generateJavaFile(PrintStream printStream,
                                        String packageName,
                                        Set<String> imports,
                                        List<ColumnDetails> columnDetailses,
                                        String classSimpleName,
                                        String tableName,
                                        boolean isView,
                                        boolean includeCloneMethod) {
        printStream.printf("package %s;\n", packageName);
        if (imports.size() > 0) {
            printStream.printf("\n");
            String previousPackageAncestor = null;
            for (String anImport : imports) {
                String packageAncestor = anImport.replaceFirst("\\.", "!").replaceFirst("\\..*$", "").replace('!', '.');
                if (previousPackageAncestor != null && !previousPackageAncestor.equals(packageAncestor)) {
                    printStream.printf("\n");
                }
                printStream.printf("%s\n", anImport);
                previousPackageAncestor = packageAncestor;
            }
        }
        printStream.printf("\n");
        if (tableName != null && !isView) {
            printStream.printf("@TableRow(tableName = \"%s\")\n", tableName);
        }
        printStream.printf("public class %s {\n", classSimpleName);
        printStream.printf("\n");
        for (ColumnDetails columnDetails : columnDetailses) {
            printStream.printf("    private %s %s;\n", columnDetails.javaClassSimpleName, columnDetails.fieldName);
        }
        for (ColumnDetails columnDetails : columnDetailses) {
            printStream.printf("\n");
            if (columnDetails.isInPrimaryKey) {
                printStream.printf("    @Id\n");
            }
            printStream.printf("    public %s get%s() {\n", columnDetails.javaClassSimpleName, columnDetails.leadingUCFieldName);
            printStream.printf("        return %s;\n", columnDetails.fieldName);
            printStream.printf("    }\n");
            printStream.printf("\n");
            printStream.printf("    public %s set%s(%s %s) {\n", classSimpleName, columnDetails.leadingUCFieldName, columnDetails.javaClassSimpleName, columnDetails.fieldName);
            printStream.printf("        this.%s = %s;\n", columnDetails.fieldName, columnDetails.fieldName);
            printStream.printf("        return this;\n");
            printStream.printf("    }\n");
        }
        if (includeCloneMethod) {
            printStream.printf("\n");
            printStream.printf("    public %s clone() {\n", classSimpleName);
            printStream.printf("        %s clone = new %s();\n", classSimpleName, classSimpleName);
            for (ColumnDetails columnDetails : columnDetailses) {
                printStream.printf("        clone.set%s(this.get%s());\n", columnDetails.leadingUCFieldName, columnDetails.leadingUCFieldName);
            }
            printStream.printf("        return clone;\n");
            printStream.printf("    }\n");
        }
        printStream.printf("\n");
        printStream.printf("}");
    }

    private static List<String> getPkColumnNames(Connection connection, String catalog, String schema, String table) {
        List<String> list = new ArrayList<String>();
        try {
            ResultSet resultSet = connection.getMetaData().getPrimaryKeys(catalog, schema, table);
            try {

                while (resultSet.next()) {
                    list.add(resultSet.getString("COLUMN_NAME"));
                }

            } finally {
                resultSet.close();
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
        return list;
    }

    private static List<ColumnDetails> getColumnDetailses(Connection connection, String tableSchema, String tableName, List<String> pkColumnNames, SortedSet<String> imports) {
        List<ColumnDetails> columnDetailses = new ArrayList<ColumnDetails>();
        try {
            ResultSet resultSet = connection.getMetaData().getColumns(null, tableSchema, tableName, null);
            try {

                while (resultSet.next()) {
                    String columnName = resultSet.getString("COLUMN_NAME");
                    columnDetailses.add(new ColumnDetails(columnName,
                                                          resultSet.getInt("DATA_TYPE"),
                                                          Strings.isIn(columnName, pkColumnNames),
                                                          imports));
                }

            } finally {
                resultSet.close();
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
        return columnDetailses;
    }


    private static class ColumnDetails {

        public String name;
        public String fieldName;
        public String leadingUCFieldName;
        public String javaClassSimpleName;
        public boolean isInPrimaryKey;

        private ColumnDetails(String name, int sqlType, boolean isInPrimaryKey, SortedSet<String> imports) {
            this.name = name;
            this.fieldName = toCamelHumpName(name, false);
            this.leadingUCFieldName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
            this.isInPrimaryKey = isInPrimaryKey;
            if (sqlType == Types.VARCHAR
                || sqlType == Types.CLOB) {
                this.javaClassSimpleName = "String";
            } else if (sqlType == Types.BIGINT) {
                this.javaClassSimpleName = "Long";
            } else if (sqlType == Types.NUMERIC
                       || sqlType == Types.FLOAT
                       || sqlType == Types.REAL
                       || sqlType == Types.DECIMAL
                       || sqlType == Types.DOUBLE) {
                this.javaClassSimpleName = "BigDecimal";
                imports.add("import java.math.*;");
            } else if (sqlType == Types.BIT
                       || sqlType == Types.BOOLEAN) {
                this.javaClassSimpleName = "Boolean";
            } else if (sqlType == Types.BLOB) {
                this.javaClassSimpleName = "Object";
            } else if (sqlType == Types.INTEGER
                       || sqlType == Types.TINYINT
                       || sqlType == Types.SMALLINT) {
                this.javaClassSimpleName = "Integer";
            } else if (sqlType == Types.DATE || sqlType == Types.TIMESTAMP) {
                if (fieldName.endsWith("Date")) {
                    this.javaClassSimpleName = "Day";
                    imports.add("import com.jirvan.dates.*;");
                } else {
                    this.javaClassSimpleName = "Date";
                    imports.add("import java.util.*;");
                }
            } else if (sqlType == Types.BINARY) {
                this.javaClassSimpleName = "byte[]";
            } else {
                this.javaClassSimpleName = "Zzz";
//                throw new RuntimeException(String.format("Cannot handle columns sql data type %d", sqlType));
            }
            this.isInPrimaryKey = isInPrimaryKey;
            if (isInPrimaryKey) {
                imports.add("import com.jirvan.jidbc.*;");
            }
        }

    }

    private static String toCamelHumpName(String name, boolean leadingCharacterUppercase) {
        if (name.matches(".*[a-z].*") && name.matches(".*[A-Z].*")) {
            return name;
        } else {
            StringBuilder stringBuilder = new StringBuilder();
            char[] chars = name.toLowerCase().toCharArray();
            boolean firstWordLetter = leadingCharacterUppercase;
            for (int i = 0; i < chars.length; i++) {
                char c = chars[i];
                if (c == '_') {
                    firstWordLetter = true;
                } else if (firstWordLetter) {
                    if ('a' <= c && c <= 'z') {
                        stringBuilder.append((char) (c + ('A' - 'a')));
                    } else {
                        stringBuilder.append(c);
                    }
                    firstWordLetter = false;
                } else {
                    stringBuilder.append(c);
                }
            }
            return stringBuilder.toString();
        }
    }

}
