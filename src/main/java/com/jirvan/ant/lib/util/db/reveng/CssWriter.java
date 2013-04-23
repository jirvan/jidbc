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

public class CssWriter {

    public static void writeCssFile(File docDir) throws SQLException, IOException {

        PrintWriter writer = new PrintWriter(new File(docDir, "table.css"));
        try {

            writer.printf("HTML, BODY, TD, TR {font-size:12px}\n" +
                          "* {font-family: Arial, Helvetica, sans-serif;}\n" +
                          "\n" +
                          "A {\n" +
                          "    color: #2B4EB7;\n" +
                          "}\n" +
                          "\n" +
                          "A:link {\n" +
                          "    color:           #2B4EB7;\n" +
                          "    text-decoration: underline;\n" +
                          "}\n" +
                          "\n" +
                          "A:visited {\n" +
                          "    color:           #8B008B;\n" +
                          "    text-decoration: underline;\n" +
                          "}\n" +
                          "\n" +
                          "A:hover, A:visited:hover {\n" +
                          "    text-decoration: underline;\n" +
                          "    color:           #FF6600\n" +
                          "}\n" +
                          "\n" +
                          "H1 {\n" +
                          "    font-size: 24px\n" +
                          "}\n" +
                          "\n" +
                          "H1 {\n" +
                          "    color:       #666666;\n" +
                          "    font-weight: normal;\n" +
                          "}\n" +
                          "\n" +
                          "H2 {\n" +
                          "    color:   #334D79;\n" +
                          "    font-size:   18px;\n" +
                          "    font-weight: bold;\n" +
                          "    padding:     0px 0px 2px 0px;\n" +
                          "    margin:      0px;\n" +
                          "}\n" +
                          "\n" +
                          "H3 {\n" +
                          "    color:   #334D79;\n" +
                          "    padding: 10px 0px 2px 0px;\n" +
                          "    margin:  0px;\n" +
                          "}\n" +
                          "\n" +
                          "H4 {\n" +
                          "    color:   #334D79;\n" +
                          "    padding: 0px 0px 2px 0px;\n" +
                          "    margin:  0px;\n" +
                          "}\n" +
                          "\n" +
                          "LI {\n" +
                          "    margin-top: 5;\n" +
                          "}\n" +
                          "\n" +
                          ".technical {\n" +
                          "    display: block;\n" +
                          "}\n" +
                          "H4.technical {\n" +
                          "    color:   red;\n" +
                          "}\n" +
                          "\n" +
                          ".zacorgnavheading {color: #000; padding:5px 0px 0px 0px; font-weight:bold; margin:0px;}\n" +
                          "A.zacorgnavheading, A.zacorgnavheading:link {font-weight:bold; margin:0px 10px 5px 0px; padding:0px; width:auto; display:block;}");

        } finally {
            writer.close();
        }
    }

}