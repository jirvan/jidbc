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


import java.io.*;
import java.util.*;

public class JidbcInfo_noDependencies {

    protected static final ArtifactInfo jidbcInfo = new ArtifactInfo(getResourcePropertyValue(JidbcInfo_noDependencies.class, "jidbc.build.properties", "project.name"),
                                                                     getResourcePropertyValue(JidbcInfo_noDependencies.class, "jidbc.build.properties", "project.version"));

    public static final String USAGE = "\nUsage:\n\n   java -jar <jar file> [-j]";

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.printf("\n%s\n", getDetails());
        } else if (args.length == 1 && "-j".equals(args[0])) {
            System.out.printf("\n%s\n", getDetailsJson());
        } else {
            System.err.println(USAGE);
        }
    }

    public static ArtifactInfo getInfo() {
        return jidbcInfo;
    }

    public static String getName() {
        return jidbcInfo.getName();
    }

    public static String getVersion() {
        return jidbcInfo.getVersion();
    }

    public static String getDetails() {
        return String.format("%s: %s", jidbcInfo.getName(), jidbcInfo.getVersion());
    }

    public static String getDetailsJson() {
        return String.format("{\n" +
                             "    \"name\": \"%s\",\n" +
                             "    \"version\": \"%s\"\n" +
                             "}", jidbcInfo.getName(), jidbcInfo.getVersion());
    }

    private static String getResourcePropertyValue(Class anchorClass, String propertiesFileRelativePath, String key) {
        Properties properties = getProperties(anchorClass, propertiesFileRelativePath);
        String version = properties.getProperty(key);
        if (version == null) throw new RuntimeException(String.format("%s not found in %s", key, propertiesFileRelativePath));
        return version;
    }

    private static Properties getProperties(Class anchorClass, String propertiesFileRelativePath) {
        try {
            InputStream inputStream = anchorClass.getResourceAsStream(propertiesFileRelativePath);
            if (inputStream == null) {
                throw new RuntimeException("Couldn't find resource \"" + propertiesFileRelativePath + "\" associated with class \"" + anchorClass.getName() + "\"");
            }
            try {
                Properties properties = new Properties();
                properties.load(inputStream);
                return properties;
            } finally {
                inputStream.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class ArtifactInfo {

        private String name;
        private String version;

        public ArtifactInfo() {
        }

        public ArtifactInfo(String name, String version) {
            this.name = name;
            this.version = version;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getVersion() {
            return version;
        }

        public void setVersion(String version) {
            this.version = version;
        }
    }

}
