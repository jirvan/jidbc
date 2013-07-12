package com.jirvan.jidbc;


import org.testng.annotations.*;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

public class JidbcConnection_MultipleReturnedObjectTests extends TestsBase {

    @Test
    public void query() {

        // Open a separate database connection and insert a test row
        Department newDepartment = DEPARTMENT1.newInstance();
        Jidbc.insert(DATA_SOURCE, newDepartment);
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Query and check rows
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        int count = 0;
        try {
            for (Department department : jidbc.<Department>query(Department.class, "where department_abbr in (?,?)",
                                                                 DEPARTMENT1.DEPARTMENT_ABBR, DEPARTMENT2.DEPARTMENT_ABBR)) {
                if (++count == 1) {
                    assertEquals("department.department_abbr", DEPARTMENT1.DEPARTMENT_ABBR, department.departmentAbbr);
                    assertEquals("department.department_name", DEPARTMENT1.DEPARTMENT_NAME, department.departmentName);
                    assertEquals("department.creation_anniversary", DEPARTMENT1.CREATION_ANNIVERSARY, department.creationAnniversary);
                    assertEquals("department.thingy_type", DEPARTMENT1.THINGY_TYPE, department.thingyType);
                    assertEquals("department.thingy_number", DEPARTMENT1.THINGY_NUMBER, department.thingyNumber);
                    assertEquals("department.another_thingy", DEPARTMENT1.ANOTHER_THINGY, department.anotherThingy);
                    assertEquals("department.inactivated_datetime", DEPARTMENT1.INACTIVATED_DATETIME, department.inactivatedDatetime);
                } else if (count == 2) {
                    assertEquals("department.department_abbr", DEPARTMENT2.DEPARTMENT_ABBR, department.departmentAbbr);
                    assertEquals("department.department_name", DEPARTMENT2.DEPARTMENT_NAME, department.departmentName);
                    assertEquals("department.creation_anniversary", DEPARTMENT2.CREATION_ANNIVERSARY, department.creationAnniversary);
                    assertEquals("department.thingy_type", DEPARTMENT2.THINGY_TYPE, department.thingyType);
                    assertEquals("department.thingy_number", DEPARTMENT2.THINGY_NUMBER, department.thingyNumber);
                    assertEquals("department.another_thingy", DEPARTMENT2.ANOTHER_THINGY, department.anotherThingy);
                    assertEquals("department.inactivated_datetime", DEPARTMENT2.INACTIVATED_DATETIME, department.inactivatedDatetime);
                } else {
                    fail(String.format("Unexpected row count %d", count));
                }

            }
            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

        // Check the total number of rows returned
        assertEquals("total rows returned", 2, count);

    }

    @Test
    public void query_placeholders() {

        // Open a separate database connection and insert a test row
        Department newDepartment = DEPARTMENT1.newInstance();
        Jidbc.insert(DATA_SOURCE, newDepartment);
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Query and check rows
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        int count = 0;
        try {
            Object[] parameters = {DEPARTMENT1.DEPARTMENT_ABBR, DEPARTMENT2.DEPARTMENT_ABBR};
            for (Department department : jidbc.<Department>query(Department.class, "where department_abbr in ("+ Jidbc.placeholders(parameters) + ")", parameters)) {
                if (++count == 1) {
                    assertEquals("department.department_abbr", DEPARTMENT1.DEPARTMENT_ABBR, department.departmentAbbr);
                    assertEquals("department.department_name", DEPARTMENT1.DEPARTMENT_NAME, department.departmentName);
                    assertEquals("department.creation_anniversary", DEPARTMENT1.CREATION_ANNIVERSARY, department.creationAnniversary);
                    assertEquals("department.thingy_type", DEPARTMENT1.THINGY_TYPE, department.thingyType);
                    assertEquals("department.thingy_number", DEPARTMENT1.THINGY_NUMBER, department.thingyNumber);
                    assertEquals("department.another_thingy", DEPARTMENT1.ANOTHER_THINGY, department.anotherThingy);
                    assertEquals("department.inactivated_datetime", DEPARTMENT1.INACTIVATED_DATETIME, department.inactivatedDatetime);
                } else if (count == 2) {
                    assertEquals("department.department_abbr", DEPARTMENT2.DEPARTMENT_ABBR, department.departmentAbbr);
                    assertEquals("department.department_name", DEPARTMENT2.DEPARTMENT_NAME, department.departmentName);
                    assertEquals("department.creation_anniversary", DEPARTMENT2.CREATION_ANNIVERSARY, department.creationAnniversary);
                    assertEquals("department.thingy_type", DEPARTMENT2.THINGY_TYPE, department.thingyType);
                    assertEquals("department.thingy_number", DEPARTMENT2.THINGY_NUMBER, department.thingyNumber);
                    assertEquals("department.another_thingy", DEPARTMENT2.ANOTHER_THINGY, department.anotherThingy);
                    assertEquals("department.inactivated_datetime", DEPARTMENT2.INACTIVATED_DATETIME, department.inactivatedDatetime);
                } else {
                    fail(String.format("Unexpected row count %d", count));
                }

            }
            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

        // Check the total number of rows returned
        assertEquals("total rows returned", 2, count);

    }

}
