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

import java.io.*;
import java.sql.*;

public class TableReporter {

    public static void writeTableReport_html(File docDir, Table table) throws SQLException, IOException {

        String fileName = table.tableName + ".html";
        PrintWriter writer = new PrintWriter(new File(docDir, fileName));
        try {
            writeTableReport_html(writer, table);
        } finally {
            writer.close();
        }
    }

    public static void writeTableReport_html(PrintWriter writer, Table table) throws SQLException, IOException {

        // Writer first section
        writer.printf("<html>\n" +
                      "<head>\n" +
                      " <meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\"/>\n" +
                      " <link href=\"table.css\" rel=\"stylesheet\" type=\"text/css\"/>\n" +
                      " <title>The %1$s table</title>\n" +
                      "\n" +
                      "<body style=\"margin: 20px;\" bgcolor=\"#ffffff\">\n" +
                      "\n" +
                      "<H2>The %1$s table.</H2>\n" +
                      "\n" +
                      "<table cellpadding=\"5\">\n" +
                      " <tr>\n" +
                      "  <td style=\"font-weight: bold\">Number of Rows:</td>\n" +
                      "  <td>%2$s</td>\n" +
                      " </tr>\n" +
                      "</table>\n" +
                      "\n" +
                      "<h3>Columns</h3>\n" +
                      "\n" +
                      "<table border=\"1\" cellpadding=\"5\">\n", table.tableName, format(table.numRows));

        // Write column details
        writer.printf("<tr style=\"font-weight: bold\">\n" +
                      "  <td>Column</td>\n" +
                      "  <td>Data Type</td>\n" +
                      "  <td>Longest length</td>\n" +
                      "  <td>Num Distinct</td>\n" +
                      "  <td>e.g.</td>\n" +
                      " </tr>\n");
        for (Column column : table.columns) {
            writer.printf("<tr>\n" +
                          "  <td>%s</td>\n" +
                          "  <td>%s</td>\n" +
                          "  <td>%s</td>\n" +
                          "  <td>%s</td>\n" +
                          "  <td>%s</td>\n" +
                          " </tr>\n",
                          column.columnName,
                          TableReverseEngineer.formatDataDefinition(column, false),
                          format(column.longestLength),
                          format(column.numDistinctValues),
                          "?");
        }

        // Writer final section
        writer.printf("</table>\n" +
                      "\n" +
                      "</body>\n" +
                      "\n" +
                      "</html>");

    }

    public static String format(String string) {
        if (string == null) {
            return "&nbsp;";
        } else {
            return string;
        }
    }

    public static String format(Long value) {
        if (value == null) {
            return "&nbsp;";
        } else {
            return value.toString();
        }
    }

    public static String format(Integer value) {
        if (value == null) {
            return "&nbsp;";
        } else {
            return value.toString();
        }
    }

}