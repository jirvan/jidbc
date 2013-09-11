package com.jirvan.jidbc;


import com.jirvan.lang.*;
import org.testng.annotations.*;

import static org.testng.AssertJUnit.*;

public class Jidbc_SingleReturnedObjectTests extends TestsBase {

    @Test
    public void queryFor() {

        // Create and insert a test row
        Department newDepartment = DEPARTMENT1.newInstance();
        Jidbc.insert(DATA_SOURCE, newDepartment);
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Test queryFor success
        Department department = Jidbc.queryFor(DATA_SOURCE, Department.class, "where department_id = ?", newDepartment.departmentId);
        assertEquals("department.department_abbr", DEPARTMENT1.DEPARTMENT_ABBR, department.departmentAbbr);
        assertEquals("department.department_name", DEPARTMENT1.DEPARTMENT_NAME, department.departmentName);
        assertEquals("department.creation_anniversary", DEPARTMENT1.CREATION_ANNIVERSARY, department.creationAnniversary);
             assertEquals("department.some_month", DEPARTMENT1.SOME_MONTH, department.someMonth);
       assertEquals("department.thingy_type", DEPARTMENT1.THINGY_TYPE, department.thingyType);
        assertEquals("department.thingy_number", DEPARTMENT1.THINGY_NUMBER, department.thingyNumber);
        assertEquals("department.another_thingy", DEPARTMENT1.ANOTHER_THINGY, department.anotherThingy);
        assertEquals("department.inactivated_datetime", DEPARTMENT1.INACTIVATED_DATETIME, department.inactivatedDatetime);

        // Test not found
        try {
            Jidbc.queryFor(DATA_SOURCE, Department.class, "where department_id = ?", 342092348);
            fail("Expected NotFoundRuntimeException");
        } catch (NotFoundRuntimeException e) {
        }

    }

    @Test
    public void queryForOptional() {

        // Create and insert a test row
        Department newDepartment = DEPARTMENT1.newInstance();
        Jidbc.insert(DATA_SOURCE, newDepartment);
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Test
        Department department = Jidbc.queryForOptional(DATA_SOURCE, Department.class, "where department_id = ?", newDepartment.departmentId);
        assertEquals("department.department_abbr", DEPARTMENT1.DEPARTMENT_ABBR, department.departmentAbbr);
        assertEquals("department.department_name", DEPARTMENT1.DEPARTMENT_NAME, department.departmentName);
        assertEquals("department.creation_anniversary", DEPARTMENT1.CREATION_ANNIVERSARY, department.creationAnniversary);
            assertEquals("department.some_month", DEPARTMENT1.SOME_MONTH, department.someMonth);
        assertEquals("department.thingy_type", DEPARTMENT1.THINGY_TYPE, department.thingyType);
        assertEquals("department.thingy_number", DEPARTMENT1.THINGY_NUMBER, department.thingyNumber);
        assertEquals("department.another_thingy", DEPARTMENT1.ANOTHER_THINGY, department.anotherThingy);
        assertEquals("department.inactivated_datetime", DEPARTMENT1.INACTIVATED_DATETIME, department.inactivatedDatetime);

        // Test not found
        assertNull(String.format("Did not expect to find department:%d", 82340923840328l),
                   Jidbc.queryForOptional(DATA_SOURCE, Department.class, "where department_id = ?", 82340923840328l));

    }

    @Test
    public void queryFor_String() {

        // Create and insert a test row
        Department newDepartment = DEPARTMENT1.newInstance();
        Jidbc.insert(DATA_SOURCE, newDepartment);
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Test queryFor success
        assertEquals("department.department_name",
                     DEPARTMENT1.DEPARTMENT_NAME,
                     Jidbc.queryFor_String(DATA_SOURCE, "select department_name from departments where department_id = ?", newDepartment.departmentId));

        // Test not found
        try {
            Jidbc.queryFor_String(DATA_SOURCE, "select department_name from departments where department_id = ?", 342092348);
            fail("Expected NotFoundRuntimeException");
        } catch (NotFoundRuntimeException e) {
        }

    }

    @Test
    public void queryForOptional_String() {

        // Create and insert a test row
        Department newDepartment = DEPARTMENT1.newInstance();
        Jidbc.insert(DATA_SOURCE, newDepartment);
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Test queryFor success
        assertEquals("department.department_name",
                     DEPARTMENT1.DEPARTMENT_NAME,
                     Jidbc.queryForOptional_String(DATA_SOURCE, "select department_name from departments where department_id = ?", newDepartment.departmentId));

        // Test not found
        assertNull(String.format("Did not expect to find department:%d", 82340923840328l),
                   Jidbc.queryForOptional_String(DATA_SOURCE, "select department_name from departments where department_id = ?", 82340923840328l));

    }

    @Test
    public void queryFor_Long() {

        // Create and insert a test row
        Jidbc.insert(DATA_SOURCE, DEPARTMENT1.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Test queryFor success
        assertEquals("department.department_name",
                     DEPARTMENT2.DEPARTMENT_ID,
                     Jidbc.queryFor_Long(DATA_SOURCE, "select department_id from departments where department_id = ?", DEPARTMENT2.DEPARTMENT_ID));

        // Test not found
        try {
            Jidbc.queryFor_Long(DATA_SOURCE, "select department_id from departments where department_id = ?", 342092348);
            fail("Expected NotFoundRuntimeException");
        } catch (NotFoundRuntimeException e) {
        }

    }

    @Test
    public void queryForOptional_Long() {

        // Create and insert a test row
        Jidbc.insert(DATA_SOURCE, DEPARTMENT1.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Test queryFor success
        assertEquals("department.department_id",
                     DEPARTMENT2.DEPARTMENT_ID,
                     Jidbc.queryForOptional_Long(DATA_SOURCE, "select department_id from departments where department_id = ?", DEPARTMENT2.DEPARTMENT_ID));

        // Test not found
        assertNull(String.format("Did not expect to find department:%d", 82340923840328l),
                   Jidbc.queryForOptional_Long(DATA_SOURCE, "select department_id from departments where department_id = ?", 82340923840328l));

    }

    @Test
    public void queryFor_BigDecimal() {

        // Create and insert a test row
        Jidbc.insert(DATA_SOURCE, DEPARTMENT1.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Test queryFor success
        assertEquals("department.department_name",
                     DEPARTMENT2.ANOTHER_THINGY,
                     Jidbc.queryFor_BigDecimal(DATA_SOURCE, "select another_thingy from departments where department_id = ?", DEPARTMENT2.DEPARTMENT_ID));

        // Test not found
        try {
            Jidbc.queryFor_BigDecimal(DATA_SOURCE, "select department_id from departments where department_id = ?", 342092348);
            fail("Expected NotFoundRuntimeException");
        } catch (NotFoundRuntimeException e) {
        }

    }

    @Test
    public void queryForOptional_BigDecimal() {

        // Create and insert a test row
        Jidbc.insert(DATA_SOURCE, DEPARTMENT1.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Test queryFor success
        assertEquals("department.department_id",
                     DEPARTMENT2.ANOTHER_THINGY,
                     Jidbc.queryForOptional_BigDecimal(DATA_SOURCE, "select another_thingy from departments where department_id = ?", DEPARTMENT2.DEPARTMENT_ID));

        // Test not found
        assertNull(String.format("Did not expect to find department:%d", 82340923840328l),
                   Jidbc.queryForOptional_BigDecimal(DATA_SOURCE, "select department_id from departments where department_id = ?", 82340923840328l));

    }

    @Test
    public void queryFor_Day() {

        // Create and insert a test row
        Jidbc.insert(DATA_SOURCE, DEPARTMENT1.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Test queryFor success
        assertEquals("department.creation_anniversary",
                     DEPARTMENT2.CREATION_ANNIVERSARY,
                     Jidbc.queryFor_Day(DATA_SOURCE, "select creation_anniversary from departments where department_id = ?", DEPARTMENT2.DEPARTMENT_ID));

        // Test not found
        try {
            Jidbc.queryFor_Day(DATA_SOURCE, "select creation_anniversary from departments where department_id = ?", 342092348);
            fail("Expected NotFoundRuntimeException");
        } catch (NotFoundRuntimeException e) {
        }

    }

    @Test
    public void queryForOptional_Day() {

        // Create and insert a test row
        Jidbc.insert(DATA_SOURCE, DEPARTMENT1.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Test queryFor success
        assertEquals("department.creation_anniversary",
                     DEPARTMENT2.CREATION_ANNIVERSARY,
                     Jidbc.queryForOptional_Day(DATA_SOURCE, "select creation_anniversary from departments where department_id = ?", DEPARTMENT2.DEPARTMENT_ID));

        // Test not found
        assertNull(String.format("Did not expect to find department:%d", 82340923840328l),
                   Jidbc.queryForOptional_Day(DATA_SOURCE, "select department_id from departments where department_id = ?", 82340923840328l));

    }

}
