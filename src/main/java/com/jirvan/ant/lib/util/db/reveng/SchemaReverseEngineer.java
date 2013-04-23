/*

Copyright (c) 2008, Jirvan Pty Ltd
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

import com.jirvan.lang.*;
import com.jirvan.util.*;

import javax.sql.*;
import java.io.*;
import java.sql.*;

import static com.jirvan.util.Assertions.assertTrue;


public class SchemaReverseEngineer {

    public static void main(String[] args) {
        if (false) {
            reverseEngineerToASingleFile(Jdbc.getPostgresDataSource("zac/x@gdansk/zacdev"),
                                         "public",
                                         new File("l:/Desktop/gen/zacdb/zacdb-recreate.sql"),
                                         true);
        } else {
            reverseEngineer(Jdbc.getPostgresDataSource("zac/x@gdansk/zacdev"),
                            "public",
                            new File("l:/Desktop/gen/zacdb/dirTree"),
                            true);
        }
    }

    public static void reverseEngineer(DataSource dataSource,
                                       String schemaName,
                                       File outDir,
                                       boolean replaceExisting) {
        reverseEngineer(dataSource,
                        schemaName,
                        null, // tablesToInclude,
                        null, // viewsToInclude,
                        new DatatypeMap[]{
                                new DatatypeMap(new DatatypeMap.DataTypeDef("int8", 19, 0),
                                                new DatatypeMap.DataTypeDef("bigint", null, null)),
                                new DatatypeMap(new DatatypeMap.DataTypeDef("varchar", -1, 0),
                                                new DatatypeMap.DataTypeDef("varchar", -1, null))
                        },
                        outDir,
                        null, // outFile,
                        true, // lowerCaseNames,
                        true, // lowerCaseDataTypes,
                        replaceExisting,
                        false, // noCheckConstaints,
                        false, // viewsAsTables,
                        true, // includeDocs,
                        false); // includeAntBuildScript);
    }

    public static void reverseEngineerToASingleFile(DataSource dataSource,
                                                    String schemaName,
                                                    File outFile,
                                                    boolean replaceExisting) {
        reverseEngineer(dataSource,
                        schemaName,
                        null, // tablesToInclude,
                        null, // viewsToInclude,
                        new DatatypeMap[]{
                                new DatatypeMap(new DatatypeMap.DataTypeDef("int8", 19, 0),
                                                new DatatypeMap.DataTypeDef("bigint", null, null)),
                                new DatatypeMap(new DatatypeMap.DataTypeDef("varchar", -1, 0),
                                                new DatatypeMap.DataTypeDef("varchar", -1, null))
                        },
                        null,    // outDir,
                        outFile, // outFile,
                        true, // lowerCaseNames,
                        true, // lowerCaseDataTypes,
                        replaceExisting,
                        false, // noCheckConstaints,
                        false, // viewsAsTables,
                        true, // includeDocs,
                        false); // includeAntBuildScript);
    }

    public static void reverseEngineer(DataSource dataSource,
                                       String schemaName,
                                       String[] tablesToInclude,
                                       String[] viewsToInclude,
                                       DatatypeMap[] mapdatatypes,
                                       File outDir,
                                       File outFile,
                                       boolean lowerCaseNames,
                                       boolean lowerCaseDataTypes,
                                       boolean replaceExisting,
                                       boolean noCheckConstaints,
                                       boolean viewsAsTables,
                                       boolean includeDocs,
                                       boolean includeAntBuildScript) {
        try {
            Connection conn = dataSource.getConnection();
            try {
                reverseEngineer(conn,
                                schemaName,
                                tablesToInclude,
                                viewsToInclude,
                                mapdatatypes,
                                outDir,
                                outFile,
                                lowerCaseNames,
                                lowerCaseDataTypes,
                                replaceExisting,
                                noCheckConstaints,
                                viewsAsTables,
                                includeDocs,
                                includeAntBuildScript);
            } finally {
                conn.close();
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void reverseEngineer(Connection conn,
                                       String schemaName,
                                       String[] tablesToInclude,
                                       String[] viewsToInclude,
                                       DatatypeMap[] mapdatatypes,
                                       File outDir,
                                       File outFile,
                                       boolean lowerCaseNames,
                                       boolean lowerCaseDataTypes,
                                       boolean replaceExisting,
                                       boolean noCheckConstaints,
                                       boolean viewsAsTables,
                                       boolean includeDocs,
                                       boolean includeAntBuildScript) throws SQLException, IOException {
        CaseSetter nameCase = lowerCaseNames ? new LowerCaseSetter() : new CaseSetter();

        // Setup output directory and get schema structure
        File tablesDir;
        File foreignKeysDir;
        if (outDir != null) {
            if (outDir.exists()) {
                if (replaceExisting) {
                    //Io.deleteRecursive(outDir);
                } else {
                    throw new RuntimeException("Output directory \"" + outDir.getAbsolutePath() + "\" already exists");
                }
            }
            outDir.mkdirs();
            tablesDir = new File(outDir, "tables");
            foreignKeysDir = new File(outDir, "foreignKeys");
        } else if (outFile != null) {
            if (!outFile.getParentFile().exists()) {
                outFile.getParentFile().mkdirs();
            }
            tablesDir = null;
            foreignKeysDir = null;
        } else {
            throw new RuntimeException("Either outDir or outFile must be provided");
        }
        File docDir = new File(outDir, "doc");
        Schema schema = Schema.get(conn, conn.getMetaData(), schemaName, tablesToInclude, viewsToInclude, mapdatatypes, noCheckConstaints, includeDocs);

        // Write overall ant build file
        if (includeAntBuildScript && outDir != null) {
            File antBuildFile = new File(outDir, nameCase.set(schema.schemaName) + ".schema.xml");
            if (antBuildFile.exists()) {
                assertTrue(replaceExisting, String.format("File \"%s\" exists", antBuildFile.getAbsolutePath()));
                antBuildFile.delete();
            }
            writeAntBuildFile(antBuildFile, schema, nameCase, viewsAsTables);
        }

        // Delete existing output file and open new file output writer if appropriate
        PrintWriter outFileWriter = null;
        if (outFile != null) {
            if (outFile.exists()) {
                assertTrue(replaceExisting, String.format("File \"%s\" exists", outFile.getAbsolutePath()));
                outFile.delete();
            }
            outFileWriter = new PrintWriter(outFile);
        }
        try {

            // Write table creation scripts
            if (outFileWriter != null) {
                if (schema.tables.length > 0 || (viewsAsTables && schema.views.length > 0)) {
                    outFileWriter.printf("-- Create tables");
                }
                if (schema.tables.length > 0) {
                    for (Table table : schema.tables) {
                        outFileWriter.printf("\n\n");
                        TableReverseEngineer.reverseEngineer(outFileWriter, table, nameCase, lowerCaseDataTypes);
                    }
                }
                if (viewsAsTables) { // Write view derived table creation scripts if requested
                    if (schema.views.length > 0) {
                        for (View view : schema.views) {
                            outFileWriter.printf("\n\n");
                            TableReverseEngineer.reverseEngineerAsTable(outFileWriter, view, nameCase, lowerCaseDataTypes);
                        }
                    }
                }
            } else {
                Io.deleteFiles(tablesDir, "^CrTab_.*\\.sql$");
                if (schema.tables.length > 0 || (viewsAsTables && schema.views.length > 0)) {
                    tablesDir.mkdir();
                }
                if (schema.tables.length > 0) {
                    for (Table table : schema.tables) {
                        TableReverseEngineer.reverseEngineer(tablesDir, table, nameCase, lowerCaseDataTypes);
                    }
                }
                if (viewsAsTables) { // Write view derived table creation scripts if requested
                    if (schema.views.length > 0) {
                        for (View view : schema.views) {
                            TableReverseEngineer.reverseEngineerAsTable(tablesDir, view, nameCase, lowerCaseDataTypes);
                        }
                    }
                }
            }

            // Write foreign key creation scripts
            if (outFileWriter != null) {
                if (schema.tablesWithForeignKeys.length > 0) {
                    outFileWriter.printf("\n\n\n-- Create foreign keys");
                    for (Table table : schema.tablesWithForeignKeys) {
                        outFileWriter.printf("\n\n");
                        TableReverseEngineer.reverseEngineerForeignKeys(outFileWriter, table, nameCase);
                    }
                }
            } else {
                Io.deleteFiles(foreignKeysDir, "^CrFKeysFor_.*\\.sql$");
                if (schema.tablesWithForeignKeys.length > 0) {
                    foreignKeysDir.mkdir();
                    for (Table table : schema.tablesWithForeignKeys) {
                        TableReverseEngineer.reverseEngineerForeignKeys(foreignKeysDir, table, nameCase);
                    }
                }
            }

            // Write documentation
            if (includeDocs) {
                docDir.mkdir();
                CssWriter.writeCssFile(docDir);
                for (Table table : schema.tables) {
                    TableReporter.writeTableReport_html(docDir, table);
                }
            }

            int numTablesGenerated = viewsAsTables ? schema.tables.length + schema.views.length : schema.tables.length;
            if (schema.tablesWithForeignKeys.length > 0) {
                System.out.printf("Reverse engineered %d tables, with foreign keys for %d of them.", numTablesGenerated, schema.tablesWithForeignKeys.length);
            } else {
                System.out.printf("Reverse engineered %d tables.", numTablesGenerated);
            }


        } finally {
            if (outFileWriter != null) outFileWriter.close();
        }

    }

    private static void writeAntBuildFile(File antBuildFile, Schema schema, CaseSetter nameCase, boolean viewsAsTables) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(antBuildFile);
        try {
            writer.printf("<?xml version=\"1.0\"?>\n" +
                          "\n" +
                          "<project name=\"%s\" basedir=\".\">\n" +
                          "\n" +
                          "    <property file=\"${user.home}/.%s.properties\"/>\n" +
                          "    <taskdef resource=\"ji-ant.tasks\"/>\n" +
                          "\n" +
                          "    <target name=\"createSchema\" depends=\"createTables,createForeignKeys\"/>\n" +
                          "\n" +
                          "    <target name=\"createTables\">\n" +
                          "        <jisql postgresconn=\"${app.connection}\">\n",
                          antBuildFile.getName(),
                          nameCase.set(schema.schemaName));
            for (Table table : schema.tables) {
                writer.printf("            <transaction src=\"tables/CrTab_%s.sql\"/>\n", nameCase.set(table.tableName));
            }
            if (viewsAsTables) { // Write view derived table creation scripts if requested
                for (View table : schema.views) {
                    writer.printf("            <transaction src=\"tables/CrTab_%s.sql\"/>\n", nameCase.set(table.viewName));
                }
            }
            writer.printf("        </jisql>\n" +
                          "    </target>\n");
            if (schema.tablesWithForeignKeys.length > 0) {
                writer.printf("\n" +
                              "    <target name=\"createForeignKeys\">\n" +
                              "        <jisql postgresconn=\"${app.connection}\">\n");
                for (Table table : schema.tablesWithForeignKeys) {
                    writer.printf("            <transaction src=\"foreignkeys/CrFKeysFor_%s.sql\"/>\n", nameCase.set(table.tableName));
                }
                writer.printf("        </jisql>\n" +
                              "    </target>");
            }
            writer.printf("\n" +
                          "\n" +
                          "</project>");
        } finally {
            writer.close();
        }
    }

}
