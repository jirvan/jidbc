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

package com.jirvan.jidbc;

import com.jirvan.lang.*;
import com.jirvan.util.*;

import javax.sql.*;
import java.io.*;
import java.sql.*;
import java.util.*;

import static com.jirvan.util.Assertions.assertFileDoesNotExist;

public class JidbcRowClassGenerator {


    public static void main(String[] args) {
        generateJavaFilesIntoDirectory(Jdbc.getPostgresDataSource("kfund/x@gdansk/kfunddev"),
                                       "au.com.knowledgefund.core.db2",
                                       new File("L:\\dev\\knowledgefund\\knowledgefund-core\\src\\main\\java\\au\\com\\knowledgefund\\core\\db2"));
    }

    public static void generateJavaFilesIntoDirectory(DataSource dataSource, String packageName, File outputDirectory) {
        try {

            Connection connection = dataSource.getConnection();
            try {
                ResultSet resultSet = connection.getMetaData().getTables(null, null, null, new String[]{"TABLE"});
                try {

                    while (resultSet.next()) {
                        String tableName = resultSet.getString("TABLE_NAME");
                        String classSimpleName = toCamelHumpName(tableName, true) + "Row";
                        String tableSchema = resultSet.getString("TABLE_SCHEM");
                        Set<String> imports = new HashSet<String>();
                        List<ColumnDetails> columnDetailses = getColumnDetailses(connection, tableSchema, tableName, imports);
                        try {
                            File outputJavaFile = new File(outputDirectory, classSimpleName + ".java");
                            assertFileDoesNotExist(outputJavaFile);
                            PrintStream printStream = new PrintStream(outputJavaFile);
                            try {
                                generateJavaFile(printStream, packageName, imports, columnDetailses, classSimpleName);
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

    public static void generateJavaFile(PrintStream printStream, String packageName, Set<String> imports, List<ColumnDetails> columnDetailses, String classSimpleName) {
        printStream.printf("package %s;\n", packageName);
        if (imports.size() > 0) {
            printStream.printf("\n");
            for (String anImport : imports) {
                printStream.printf("%s\n", anImport);
            }
        }
        printStream.printf("\n");
        printStream.printf("public class %s {\n", classSimpleName);
        printStream.printf("\n");
        for (ColumnDetails columnDetails : columnDetailses) {
            printStream.printf("    private %s %s;\n", columnDetails.javaClassSimpleName, columnDetails.fieldName);
        }
        for (ColumnDetails columnDetails : columnDetailses) {
            printStream.printf("\n");
            printStream.printf("    public %s get%s() {\n", columnDetails.javaClassSimpleName, columnDetails.leadingUCFieldName);
            printStream.printf("        return %s;\n", columnDetails.fieldName);
            printStream.printf("    }\n");
            printStream.printf("\n");
            printStream.printf("    public void set%s(%s %s) {\n", columnDetails.leadingUCFieldName, columnDetails.javaClassSimpleName, columnDetails.fieldName);
            printStream.printf("        this.%s = %s;\n", columnDetails.fieldName, columnDetails.fieldName);
            printStream.printf("    }\n");
        }
        printStream.printf("\n");
        printStream.printf("}");
    }

    private static List<ColumnDetails> getColumnDetailses(Connection connection, String tableSchema, String tableName, Set<String> imports) {
        List<ColumnDetails> columnDetailses = new ArrayList<ColumnDetails>();
        try {
            ResultSet resultSet = connection.getMetaData().getColumns(null, tableSchema, tableName, null);
            try {

                while (resultSet.next()) {
                    columnDetailses.add(new ColumnDetails(resultSet.getString("COLUMN_NAME"), resultSet.getInt("DATA_TYPE"), imports));
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

        private ColumnDetails(String name, int sqlType, Set<String> imports) {
            this.name = name;
            this.fieldName = toCamelHumpName(name, false);
            this.leadingUCFieldName = fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
            if (sqlType == Types.VARCHAR) {
                this.javaClassSimpleName = "String";
            } else if (sqlType == Types.BIGINT) {
                this.javaClassSimpleName = "Long";
            } else if (sqlType == Types.NUMERIC) {
                this.javaClassSimpleName = "BigDecimal";
                imports.add("import java.math.*;");
            } else if (sqlType == Types.BIT) {
                this.javaClassSimpleName = "Boolean";
            } else if (sqlType == Types.INTEGER) {
                this.javaClassSimpleName = "Integer";
            } else if (sqlType == Types.DATE) {
                this.javaClassSimpleName = "Date";
                imports.add("import java.util.*;");
            } else if (sqlType == Types.TIMESTAMP) {
                this.javaClassSimpleName = "Date";
                imports.add("import java.util.*;");
            } else {
                throw new RuntimeException(String.format("Cannot handle columns sql data type %d", sqlType));
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
                    stringBuilder.append((char) (c + ('A' - 'a')));
                    firstWordLetter = false;
                } else {
                    stringBuilder.append(c);
                }
            }
            return stringBuilder.toString();
        }
    }

}
