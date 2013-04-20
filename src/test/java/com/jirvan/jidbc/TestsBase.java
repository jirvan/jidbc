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

import com.jirvan.util.*;
import org.testng.annotations.*;

import javax.sql.*;
import java.math.*;
import java.util.*;

import static org.testng.AssertJUnit.assertEquals;

public class TestsBase {

    protected final static DataSource DATA_SOURCE = Jdbc.getPostgresDataSource("zac/x@gdansk/zacdev");
    private static final String TEST_DATABASE_RECREATE_SCRIPT = Io.getResourceFileString(CRUDTests.class, "testDatabaseRecreateScript.sql");


    protected final static GetterSetterDepartment GETTER_SETTER_DEPARTMENT1 = createGetterSetterDepartment1();

    protected final static long DEPARTMENT2_ID = 42;
    protected final static String DEPARTMENT2_ABBR = "HR";
    protected final static String DEPARTMENT2_NAME = "Human Resources";
    protected final static Date DEPARTMENT2_INACTIVATED_DATETIME = new GregorianCalendar(2012, 5, 3).getTime();

    protected final static long DEPARTMENT3_ID = 423636;
    protected final static String DEPARTMENT3_ABBR = "TR";
    protected final static String DEPARTMENT3_NAME = "Threat Resolution";
    protected final static Date DEPARTMENT3_INACTIVATED_DATETIME = new GregorianCalendar(2012, 8, 1).getTime();

    public static class DEPARTMENT1 {

        public static final Long departmentId = null;

        public static final String departmentAbbr = "HR";

        public static final String departmentName = "Human Resources";

        public static final String thingyType = "Strawberry";

        public static final Integer thingyNumber = 42;

        public static final BigDecimal anotherThingy = new BigDecimal("42.58");

        public static final Date inactivatedDatetime = new GregorianCalendar(2012, 5, 1).getTime();

        public static Department newInstance() {
            Department department = new Department();
            department.departmentAbbr = departmentAbbr;
            department.departmentName = departmentName;
            department.thingyType = thingyType;
            department.thingyNumber = thingyNumber;
            department.anotherThingy = anotherThingy;
            department.inactivatedDatetime = inactivatedDatetime;
            return department;
        }

    }

    private static GetterSetterDepartment createGetterSetterDepartment1() {
        GetterSetterDepartment department = new GetterSetterDepartment();
        department.setDepartmentAbbr("HR");
        department.setDepartmentName("Human Resources");
        department.setThingyType("Strawberry");
        department.setThingyNumber(42);
        department.setAnotherThingy(new BigDecimal("42.58"));
        department.setInactivatedDatetime(new GregorianCalendar(2012, 5, 1).getTime());
        return department;
    }

    @BeforeMethod
    protected void beforeClass() throws Exception {
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            for (String statement : TEST_DATABASE_RECREATE_SCRIPT.replaceAll("(?m)^\\s+--.*$", "")
                                                                 .replaceAll("^\\s*\\n+", "")
                                                                 .replaceAll("(?m);\\s*\\n\\s*", ";\n")
                                                                 .split("(?m); *\\n")) {
                jidbc.executeUpdate(statement);
            }
//            TableDef.deregisterRowClasses();
//            TableDef.registerRowClass(DepartmentTwo.class, "departmentId").setGeneratorSequence("common_id_sequence");

        } finally {
            jidbc.release();
        }
    }

    protected void retrieveFromDatabaseAndAssertAttributeValuesAreEqualToDepartment1(long newDepartmentId) {
        JidbcConnection jidbc2 = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = jidbc2.queryFor(Department.class, "where department_id = ?", newDepartmentId);
            assertEquals("department.department_abbr", DEPARTMENT1.departmentAbbr, department.departmentAbbr);
            assertEquals("department.department_name", DEPARTMENT1.departmentName, department.departmentName);
            assertEquals("department.thingy_type", DEPARTMENT1.thingyType, department.thingyType);
            assertEquals("department.thingy_number", DEPARTMENT1.thingyNumber, department.thingyNumber);
            assertEquals("department.another_thingy", DEPARTMENT1.anotherThingy, department.anotherThingy);
            assertEquals("department.inactivated_datetime", DEPARTMENT1.inactivatedDatetime, department.inactivatedDatetime);

        } finally {
            jidbc2.release();
        }
    }

    protected void retrieveFromDatabaseAndAssertAttributeValuesAreEqualToGetterSetterDepartment1(long newDepartmentId) {
        JidbcConnection jidbc2 = JidbcConnection.from(DATA_SOURCE);
        try {

            GetterSetterDepartment department = jidbc2.queryFor(GetterSetterDepartment.class, "where department_id = ?", newDepartmentId);
            assertEquals("department.department_abbr", DEPARTMENT1.departmentAbbr, department.getDepartmentAbbr());
            assertEquals("department.department_name", DEPARTMENT1.departmentName, department.getDepartmentName());
            assertEquals("department.thingy_type", DEPARTMENT1.thingyType, department.getThingyType());
            assertEquals("department.thingy_number", DEPARTMENT1.thingyNumber, department.getThingyNumber());
            assertEquals("department.another_thingy", DEPARTMENT1.anotherThingy, department.getAnotherThingy());
            assertEquals("department.inactivated_datetime", DEPARTMENT1.inactivatedDatetime, department.getInactivatedDatetime());

        } finally {
            jidbc2.release();
        }
    }

    protected long insertDepartment1() {
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {
            Department department = DEPARTMENT1.newInstance();
            jidbc.insert(department);
            return department.departmentId;
        } finally {
            jidbc.release();
        }
    }

}
