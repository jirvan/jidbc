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

import com.jirvan.dates.*;
import com.jirvan.jidbc.*;

import java.math.*;
import java.util.*;

import static com.jirvan.util.Assertions.assertNotNull;

public class FieldValueHandler {

    public static abstract class ValueAction {

        public abstract void performWith(String value);

        public abstract void performWith(Integer value);

        public abstract void performWith(Long value);

        public abstract void performWith(BigDecimal value);

        public abstract void performWith(Boolean value);

        public abstract void performWith(Date value);

        public abstract void performWith(Day value);

    }

    public static void performWithValue(Object value, ValueAction actionSet) {
        assertNotNull(value, "value cannot be null if a fieldClass is not provided");
        performWithValue(value.getClass(), value, actionSet);
    }

    public static void performWithClass(Class fieldClass, ValueAction actionSet) {
        performWithValue(fieldClass, null, actionSet);
    }

    public static void performWithValue(Class fieldClass, Object value, ValueAction actionSet) {
        if (fieldClass == String.class) {
            actionSet.performWith((String) value);
        } else if (fieldClass == Integer.class) {
            actionSet.performWith((Integer) value);
        } else if (fieldClass == Long.class) {
            actionSet.performWith((Long) value);
        } else if (fieldClass == BigDecimal.class) {
            actionSet.performWith((BigDecimal) value);
        } else if (fieldClass == Boolean.class) {
            actionSet.performWith((Boolean) value);
        } else if (fieldClass == Date.class) {
            actionSet.performWith((Date) value);
        } else if (fieldClass == Day.class) {
            actionSet.performWith((Day) value);
        } else {
            throw new UnsupportedDataTypeException(String.format("%s is an unsupported type",
                                                                 fieldClass.getName()));
        }
    }

}
