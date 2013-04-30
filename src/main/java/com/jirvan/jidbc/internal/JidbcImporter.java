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

import java.io.*;

import static com.jirvan.util.Assertions.assertFileExists;

public class JidbcImporter {

    public static int importTableDataFromJsonFile(JidbcConnection jidbc, Class rowClass, File inputJsonFile) {
        assertFileExists(inputJsonFile);
        String jsonString = Io.getFileString(inputJsonFile);
        return importTableDataFromJsonString(jidbc, rowClass, jsonString);
    }

    public static int importTableDataFromJsonString(JidbcConnection jidbc, Class rowClass, String jsonString) {

        // Determine array class
        Class rowArrayClass = null;
        try {
            rowArrayClass = Class.forName("[L" + rowClass.getName() + ";");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        // Get the rows and insert them
        Object[] objects = (Object[]) Json.fromJsonString(jsonString, rowArrayClass);    // Todo read from stream
        int count = 0;
        for (Object row : objects) {
            jidbc.insert(row);
            count++;
        }

        return count;

    }

}
