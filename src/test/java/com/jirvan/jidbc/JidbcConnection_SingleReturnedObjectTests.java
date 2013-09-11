package com.jirvan.jidbc;


import com.jirvan.lang.*;
import org.testng.annotations.*;

import java.util.*;

import static org.testng.AssertJUnit.*;

public class JidbcConnection_SingleReturnedObjectTests extends TestsBase {

    @Test
    public void queryFor() {

        // Open a separate database connection and insert a test row
        Department newDepartment = DEPARTMENT1.newInstance();
        Jidbc.insert(DATA_SOURCE, newDepartment);

        // Re-open the database connection and check the inserted row
        JidbcConnection jidbc2 = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = jidbc2.queryFor(Department.class, "where department_id = ?", newDepartment.departmentId);
            assertEquals("department.department_abbr", DEPARTMENT1.DEPARTMENT_ABBR, department.departmentAbbr);
            assertEquals("department.department_name", DEPARTMENT1.DEPARTMENT_NAME, department.departmentName);
            assertEquals("department.creation_anniversary", DEPARTMENT1.CREATION_ANNIVERSARY, department.creationAnniversary);
            assertEquals("department.some_month", DEPARTMENT1.SOME_MONTH, department.someMonth);
            assertEquals("department.thingy_type", DEPARTMENT1.THINGY_TYPE, department.thingyType);
            assertEquals("department.thingy_number", DEPARTMENT1.THINGY_NUMBER, department.thingyNumber);
            assertEquals("department.another_thingy", DEPARTMENT1.ANOTHER_THINGY, department.anotherThingy);
            assertEquals("department.inactivated_datetime", DEPARTMENT1.INACTIVATED_DATETIME, department.inactivatedDatetime);

            jidbc2.commitAndClose();
        } catch (Throwable t) {
            throw jidbc2.rollbackCloseAndWrap(t);
        }

    }

}
