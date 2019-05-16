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

import com.jirvan.dates.Day;
import com.jirvan.dates.Hour;
import com.jirvan.dates.Millisecond;
import com.jirvan.dates.Minute;
import com.jirvan.dates.Month;
import com.jirvan.dates.Second;
import com.jirvan.jidbc.UnsupportedDataTypeException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;

import static com.jirvan.util.Assertions.*;

public class AttributeValueHandler {

    public static abstract class ClassAction {

        public abstract void performFor_String();

        public abstract void performFor_Integer();

        public abstract void performFor_Long();

        public abstract void performFor_BigDecimal();

        public abstract void performFor_Boolean();

        public abstract void performFor_Date();

        public abstract void performFor_ZonedDateTime();

        public abstract void performFor_Month();

        public abstract void performFor_Day();

        public abstract void performFor_LocalDate();

        public abstract void performFor_LocalDateTime();

        public abstract void performFor_Hour();

        public abstract void performFor_Minute();

        public abstract void performFor_Second();

        public abstract void performFor_Millisecond();

        public abstract void performFor_Enum(Class enumClass);

        public abstract void performFor_byteArray();

    }

    public static abstract class ValueAction {

        public abstract void performWith(String value);

        public abstract void performWith(Integer value);

        public abstract void performWith(Long value);

        public abstract void performWith(BigDecimal value);

        public abstract void performWith(Boolean value);

        public abstract void performWith(Date value);

        public abstract void performWith(ZonedDateTime value);

        public abstract void performWith(Month value);

        public abstract void performWith(Day value);

        public abstract void performWith(LocalDate value);

        public abstract void performWith(LocalDateTime value);

        public abstract void performWith(Hour value);

        public abstract void performWith(Minute value);

        public abstract void performWith(Second value);

        public abstract void performWith(Millisecond value);

        public abstract void performWith(Enum value);

        public abstract void performWith(byte[] value);

    }

    public static void performForClass(Class fieldClass, ClassAction classAction) {
        if (fieldClass == String.class) {
            classAction.performFor_String();
        } else if (fieldClass == Integer.class || fieldClass == int.class) {
            classAction.performFor_Integer();
        } else if (fieldClass == Long.class || fieldClass == long.class) {
            classAction.performFor_Long();
        } else if (fieldClass == BigDecimal.class) {
            classAction.performFor_BigDecimal();
        } else if (fieldClass == Boolean.class || fieldClass == boolean.class) {
            classAction.performFor_Boolean();
        } else if (fieldClass == Date.class) {
            classAction.performFor_Date();
        } else if (fieldClass == ZonedDateTime.class) {
            classAction.performFor_ZonedDateTime();
        } else if (fieldClass == Month.class) {
            classAction.performFor_Month();
        } else if (fieldClass == Day.class) {
            classAction.performFor_Day();
        } else if (fieldClass == LocalDate.class) {
            classAction.performFor_LocalDate();
        } else if (fieldClass == LocalDateTime.class) {
            classAction.performFor_LocalDateTime();
        } else if (fieldClass == Hour.class) {
            classAction.performFor_Hour();
        } else if (fieldClass == Minute.class) {
            classAction.performFor_Minute();
        } else if (fieldClass == Second.class) {
            classAction.performFor_Second();
        } else if (fieldClass == Millisecond.class) {
            classAction.performFor_Millisecond();
        } else if (fieldClass.isEnum()) {
            classAction.performFor_Enum(fieldClass);
        } else if (fieldClass == byte[].class) {
            classAction.performFor_byteArray();
        } else {
            throw new UnsupportedDataTypeException(String.format("%s is an unsupported type",
                                                                 fieldClass.getName()));
        }
    }

    public static void performWithValue(Object value, ValueAction actionSet) {
        assertNotNull(value, "value cannot be null if a fieldClass is not provided");
        performWithValue(value.getClass(), value, actionSet);
    }

    public static void performWithValue(Class fieldClass, Object value, ValueAction actionSet) {
        if (fieldClass == String.class) {
            actionSet.performWith((String) value);
        } else if (fieldClass == Integer.class || fieldClass == int.class) {
            actionSet.performWith((Integer) value);
        } else if (fieldClass == Long.class || fieldClass == long.class) {
            actionSet.performWith((Long) value);
        } else if (fieldClass == BigDecimal.class) {
            actionSet.performWith((BigDecimal) value);
        } else if (fieldClass == Boolean.class || fieldClass == boolean.class) {
            actionSet.performWith((Boolean) value);
        } else if (fieldClass == Date.class) {
            actionSet.performWith((Date) value);
        } else if (fieldClass == ZonedDateTime.class) {
            actionSet.performWith((ZonedDateTime) value);
        } else if (fieldClass == Month.class) {
            actionSet.performWith((Month) value);
        } else if (fieldClass == LocalDate.class) {
            actionSet.performWith((LocalDate) value);
        } else if (fieldClass == LocalDateTime.class) {
            actionSet.performWith((LocalDateTime) value);
        } else if (fieldClass == Day.class) {
            actionSet.performWith((Day) value);
        } else if (fieldClass == Hour.class) {
            actionSet.performWith((Hour) value);
        } else if (fieldClass == Minute.class) {
            actionSet.performWith((Minute) value);
        } else if (fieldClass == Second.class) {
            actionSet.performWith((Second) value);
        } else if (fieldClass == Millisecond.class) {
            actionSet.performWith((Millisecond) value);
        } else if (fieldClass.isEnum()) {
            actionSet.performWith((Enum) value);
        } else if (fieldClass == byte[].class) {
            actionSet.performWith((byte[]) value);
        } else {
            throw new UnsupportedDataTypeException(String.format("%s is an unsupported type",
                                                                 fieldClass.getName()));
        }
    }

}
