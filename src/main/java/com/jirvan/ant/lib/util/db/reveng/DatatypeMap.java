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

package com.jirvan.ant.lib.util.db.reveng;

import com.jirvan.util.*;

import java.math.*;

public class DatatypeMap {

    private DataTypeDef from;
    private DataTypeDef to;

    public DatatypeMap() {
    }

    public DatatypeMap(DataTypeDef from, DataTypeDef to) {
        this.from = from;
        this.to = to;
    }

    public DataTypeDef getFrom() {
        return from;
    }

    public void setFrom(DataTypeDef from) {
        this.from = from;
    }

    public DataTypeDef getTo() {
        return to;
    }

    public void setTo(DataTypeDef to) {
        this.to = to;
    }

    public static DataTypeDef mappingFor(DataTypeDef fromDataTypeDef, DatatypeMap[] datatypeMaps) {
        if (datatypeMaps != null) {
            for (DatatypeMap datatypeMap : datatypeMaps) {
                if (DataTypeDef.match(datatypeMap.from, fromDataTypeDef)) {
                    if (datatypeMap.from.getColumnSize().equals(BigDecimal.valueOf(-1))) {
                        return new DataTypeDef(datatypeMap.to.getTypeName(),
                                               fromDataTypeDef.getColumnSize(),
                                               datatypeMap.to.getDecimalDigits());
                    } else {
                        return datatypeMap.to;
                    }
                }
            }
        }
        return null;
    }

    public static class DataTypeDef {

        private String typeName;
        private BigDecimal columnSize;
        private BigDecimal decimalDigits;

//        public DataTypeDef(String typeName, String columnSize, int decimalDigits) {
//            this(typeName,
//                 "*".equals(columnSize) ? BigDecimal.valueOf(-1) : new BigDecimal(columnSize),
//                 BigDecimal.valueOf(decimalDigits));
//        }
//
//
//        public DataTypeDef(String typeName, String columnSize, BigDecimal decimalDigits) {
//            this(typeName,
//                 "*".equals(columnSize) ? BigDecimal.valueOf(-1) : new BigDecimal(columnSize),
//                 decimalDigits);
//        }

        public DataTypeDef(String typeName, int columnSize, int decimalDigits) {
            this(typeName, BigDecimal.valueOf(columnSize), BigDecimal.valueOf(decimalDigits));
        }

        public DataTypeDef(String typeName, int columnSize, BigDecimal decimalDigits) {
            this(typeName, BigDecimal.valueOf(columnSize), decimalDigits);
        }

        public DataTypeDef(String typeName, BigDecimal columnSize, BigDecimal decimalDigits) {
            this.typeName = typeName;
            this.columnSize = columnSize;
            this.decimalDigits = decimalDigits;
        }

        public String getTypeName() {
            return typeName;
        }

        public BigDecimal getColumnSize() {
            return columnSize;
        }

        public BigDecimal getDecimalDigits() {
            return decimalDigits;
        }

        public static boolean match(DataTypeDef fromDataTypeDef, DataTypeDef toDataTypeDef) {
            if (fromDataTypeDef == null) {
                return toDataTypeDef == null;
            } else {
                if (toDataTypeDef == null) {
                    return false;
                } else {
                    return Utl.areEqual(fromDataTypeDef.getTypeName(), toDataTypeDef.getTypeName())
                           && ((fromDataTypeDef.getColumnSize() != null && fromDataTypeDef.getColumnSize().equals(BigDecimal.valueOf(-1))) || Utl.areEqual(fromDataTypeDef.getColumnSize(), toDataTypeDef.getColumnSize()))
                           && Utl.areEqual(fromDataTypeDef.getDecimalDigits(), toDataTypeDef.getDecimalDigits());
                }
            }
        }

    }

}
