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

import com.jirvan.dates.*;
import com.jirvan.util.*;
import org.testng.annotations.*;

import javax.sql.*;
import java.math.*;
import java.util.*;

import static org.testng.AssertJUnit.assertEquals;

public class TestsBase {

    protected final static DataSource DATA_SOURCE = Jdbc.getPostgresDataSource("zac/x@gdansk/zacdev");
    private static final String TEST_DATABASE_RECREATE_SCRIPT = Io.getResourceFileString(JidbcConnection_CRUDTests.class, "testDatabaseRecreateScript.sql");
    private static final String TEST_DATABASE_DROP_SCRIPT = Io.getResourceFileString(JidbcConnection_CRUDTests.class, "testDatabaseDropScript.sql");


    public static class DEPARTMENT1 {

        public static final String DEPARTMENT_ABBR = "HR";

        public static final String DEPARTMENT_NAME = "Human Resources";

        public static final DepartmentType DEPARTMENT_TYPE = DepartmentType.NonCore;

        public static final Day CREATION_ANNIVERSARY = new Day(2002, 5, 4);

        public static final Month SOME_MONTH = new Month(2017, 5);

        public static final String THINGY_TYPE = "Strawberry";

        public static final Integer THINGY_NUMBER = 42;

        public static final BigDecimal ANOTHER_THINGY = new BigDecimal("42.58");

        public static final Date INACTIVATED_DATETIME = new GregorianCalendar(2012, 5, 1).getTime();

        public static Department newInstance() {
            Department department = new Department();
            department.departmentId = null;
            department.departmentType = DEPARTMENT_TYPE;
            department.departmentAbbr = DEPARTMENT_ABBR;
            department.departmentName = DEPARTMENT_NAME;
            department.creationAnniversary = CREATION_ANNIVERSARY;
            department.someMonth = SOME_MONTH;
            department.thingyType = THINGY_TYPE;
            department.thingyNumber = THINGY_NUMBER;
            department.anotherThingy = ANOTHER_THINGY;
            department.inactivatedDatetime = INACTIVATED_DATETIME;
            return department;
        }

        public static GetterSetterDepartment newGetterSetterInstance() {
            GetterSetterDepartment department = new GetterSetterDepartment();
            department.setDepartmentId(null);
            department.setDepartmentType(DEPARTMENT_TYPE);
            department.setDepartmentAbbr(DEPARTMENT_ABBR);
            department.setDepartmentName(DEPARTMENT_NAME);
            department.setCreationAnniversary(CREATION_ANNIVERSARY);
            department.setSomeMonth(SOME_MONTH);
            department.setThingyType(THINGY_TYPE);
            department.setThingyNumber(THINGY_NUMBER);
            department.setAnotherThingy(ANOTHER_THINGY);
            department.setInactivatedDatetime(INACTIVATED_DATETIME);
            return department;
        }

    }

    public static class DEPARTMENT2 {

        public static final Long DEPARTMENT_ID = 42l;

        public static final DepartmentType DEPARTMENT_TYPE = DepartmentType.NonCore;

        public static final String DEPARTMENT_ABBR = "Pers.";

        public static final String DEPARTMENT_NAME = "Personnel";

        public static final Day CREATION_ANNIVERSARY = new Day(2002, 5, 7);

        public static final Month SOME_MONTH = new Month(2014, 5);

        public static final String THINGY_TYPE = "Chocolate";

        public static final Integer THINGY_NUMBER = 58;

        public static final BigDecimal ANOTHER_THINGY = new BigDecimal("57.07");

        public static final Date INACTIVATED_DATETIME = new GregorianCalendar(2002, 3, 7).getTime();

        public static Department newInstance() {
            Department department = new Department();
            department.departmentId = DEPARTMENT_ID;
            department.departmentType = DEPARTMENT_TYPE;
            department.departmentAbbr = DEPARTMENT_ABBR;
            department.departmentName = DEPARTMENT_NAME;
            department.creationAnniversary = CREATION_ANNIVERSARY;
            department.someMonth = SOME_MONTH;
            department.thingyType = THINGY_TYPE;
            department.thingyNumber = THINGY_NUMBER;
            department.anotherThingy = ANOTHER_THINGY;
            department.inactivatedDatetime = INACTIVATED_DATETIME;
            return department;
        }

    }

    public static class DEPARTMENT3 {

        public static final Long DEPARTMENT_ID = 423636l;

        public static final DepartmentType DEPARTMENT_TYPE = DepartmentType.Core;

        public static final String DEPARTMENT_ABBR = "TR";

        public static final String DEPARTMENT_NAME = "Threat Resolution";

        public static final Day CREATION_ANNIVERSARY = new Day(2002, 5, 2);

        public static final Month SOME_MONTH = new Month(1980, 5);

        public static final String THINGY_TYPE = "Strawberry";

        public static final Integer THINGY_NUMBER = 412;

        public static final BigDecimal ANOTHER_THINGY = new BigDecimal("42.58");

        public static final Date INACTIVATED_DATETIME = new GregorianCalendar(2012, 8, 1).getTime();

        public static Department newInstance() {
            Department department = new Department();
            department.departmentId = DEPARTMENT_ID;
            department.departmentType = DEPARTMENT_TYPE;
            department.departmentAbbr = DEPARTMENT_ABBR;
            department.departmentName = DEPARTMENT_NAME;
            department.creationAnniversary = CREATION_ANNIVERSARY;
            department.someMonth = SOME_MONTH;
            department.thingyType = THINGY_TYPE;
            department.thingyNumber = THINGY_NUMBER;
            department.anotherThingy = ANOTHER_THINGY;
            department.inactivatedDatetime = INACTIVATED_DATETIME;
            return department;
        }

    }

    @BeforeMethod
    protected void beforeMethod() throws Exception {
        for (String statement : TEST_DATABASE_RECREATE_SCRIPT.replaceAll("(?m)^\\s+--.*$", "")
                                                             .replaceAll("^\\s*\\n+", "")
                                                             .replaceAll("(?m);\\s*\\n\\s*", ";\n")
                                                             .split("(?m); *\\n")) {
            Jidbc.executeUpdate(DATA_SOURCE, statement);
        }
//            TableDef.deregisterRowClasses();
//            TableDef.registerRowClass(DepartmentTwo.class, "departmentId").setGeneratorSequence("common_id_sequence");

    }

    @AfterMethod
    protected void afterMethod() throws Exception {
//        for (String statement : TEST_DATABASE_DROP_SCRIPT.replaceAll("(?m)^\\s+--.*$", "")
//                                                         .replaceAll("^\\s*\\n+", "")
//                                                         .replaceAll("(?m);\\s*\\n\\s*", ";\n")
//                                                         .split("(?m); *\\n")) {
//            Jidbc.executeUpdate(DATA_SOURCE, statement);
//        }
    }

    protected void retrieveFromDatabaseAndAssertAttributeValuesAreEqualToDepartment1(long newDepartmentId) {
        Department department = Jidbc.queryFor(DATA_SOURCE, Department.class, "where department_id = ?", newDepartmentId);
        assertEquals("department.department_abbr", DEPARTMENT1.DEPARTMENT_ABBR, department.departmentAbbr);
        assertEquals("department.department_type", DEPARTMENT1.DEPARTMENT_TYPE, department.departmentType);
        assertEquals("department.department_name", DEPARTMENT1.DEPARTMENT_NAME, department.departmentName);
        assertEquals("department.creation_anniversary", DEPARTMENT1.CREATION_ANNIVERSARY, department.creationAnniversary);
        assertEquals("department.thingy_type", DEPARTMENT1.THINGY_TYPE, department.thingyType);
        assertEquals("department.thingy_number", DEPARTMENT1.THINGY_NUMBER, department.thingyNumber);
        assertEquals("department.another_thingy", DEPARTMENT1.ANOTHER_THINGY, department.anotherThingy);
        assertEquals("department.inactivated_datetime", DEPARTMENT1.INACTIVATED_DATETIME, department.inactivatedDatetime);
    }

    protected void retrieveFromDatabaseAndAssertAttributeValuesAreEqualToGetterSetterDepartment1(long newDepartmentId) {
        GetterSetterDepartment department = Jidbc.queryFor(DATA_SOURCE, GetterSetterDepartment.class, "where department_id = ?", newDepartmentId);
        assertEquals("department.department_abbr", DEPARTMENT1.DEPARTMENT_ABBR, department.getDepartmentAbbr());
        assertEquals("department.department_name", DEPARTMENT1.DEPARTMENT_NAME, department.getDepartmentName());
        assertEquals("department.creation_anniversary", DEPARTMENT1.CREATION_ANNIVERSARY, department.getCreationAnniversary());
        assertEquals("department.thingy_type", DEPARTMENT1.THINGY_TYPE, department.getThingyType());
        assertEquals("department.thingy_number", DEPARTMENT1.THINGY_NUMBER, department.getThingyNumber());
        assertEquals("department.another_thingy", DEPARTMENT1.ANOTHER_THINGY, department.getAnotherThingy());
        assertEquals("department.inactivated_datetime", DEPARTMENT1.INACTIVATED_DATETIME, department.getInactivatedDatetime());
    }

//    protected long openASeperateConnectionAndInsertDepartment(Department department) {
//        Jidbc.insert(DATA_SOURCE, department);
//        return department.departmentId;
//    }

}
