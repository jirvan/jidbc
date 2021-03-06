package com.jirvan.jidbc;


import com.jirvan.lang.*;
import org.testng.annotations.*;

import java.util.*;

import static org.testng.AssertJUnit.*;

public class JidbcConnection_CRUDTests extends TestsBase {


    @Test
    public void insertWithExplicitId() {


        // Open a new database connection and do the insert
        long newDepartmentId;
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = new Department();
            department.departmentId = jidbc.takeSequenceNextVal("common_id_sequence");
            department.departmentType = DEPARTMENT1.DEPARTMENT_TYPE;
            department.departmentAbbr = DEPARTMENT1.DEPARTMENT_ABBR;
            department.departmentName = DEPARTMENT1.DEPARTMENT_NAME;
            department.creationAnniversary = DEPARTMENT1.CREATION_ANNIVERSARY;
            department.someMonth = DEPARTMENT1.SOME_MONTH;
            department.thingyType = DEPARTMENT1.THINGY_TYPE;
            department.thingyNumber = DEPARTMENT1.THINGY_NUMBER;
            department.anotherThingy = DEPARTMENT1.ANOTHER_THINGY;
            department.inactivatedDatetime = DEPARTMENT1.INACTIVATED_DATETIME;
            jidbc.insert(department);
            newDepartmentId = department.departmentId;

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

        // Re-open the database connection and check the inserted row
        retrieveFromDatabaseAndAssertAttributeValuesAreEqualToDepartment1(newDepartmentId);

    }

    @Test
    public void insertWithExplicitId_gettersAndSetters() {


        // Open a new database connection and do the insert
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            GetterSetterDepartment department = new GetterSetterDepartment();
            department.setDepartmentId(4242l);
            department.setDepartmentType(DEPARTMENT1.DEPARTMENT_TYPE);
            department.setDepartmentAbbr(DEPARTMENT1.DEPARTMENT_ABBR);
            department.setDepartmentName(DEPARTMENT1.DEPARTMENT_NAME);
            department.setCreationAnniversary(DEPARTMENT1.CREATION_ANNIVERSARY);
            department.setSomeMonth(DEPARTMENT1.SOME_MONTH);
            department.setThingyType(DEPARTMENT1.THINGY_TYPE);
            department.setThingyNumber(DEPARTMENT1.THINGY_NUMBER);
            department.setAnotherThingy(DEPARTMENT1.ANOTHER_THINGY);
            department.setInactivatedDatetime(DEPARTMENT1.INACTIVATED_DATETIME);
            jidbc.insert(department);

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

        // Re-open the database connection and check the inserted row
        retrieveFromDatabaseAndAssertAttributeValuesAreEqualToGetterSetterDepartment1(4242l);

    }

    @Test
    public void insertWithAutoGeneratedId() {


        // Open a new database connection and do the insert
        long newDepartmentId;
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = new Department();
            department.departmentType = DEPARTMENT1.DEPARTMENT_TYPE;
            department.departmentAbbr = DEPARTMENT1.DEPARTMENT_ABBR;
            department.departmentName = DEPARTMENT1.DEPARTMENT_NAME;
            department.creationAnniversary = DEPARTMENT1.CREATION_ANNIVERSARY;
            department.someMonth = DEPARTMENT1.SOME_MONTH;
            department.thingyType = DEPARTMENT1.THINGY_TYPE;
            department.thingyNumber = DEPARTMENT1.THINGY_NUMBER;
            department.anotherThingy = DEPARTMENT1.ANOTHER_THINGY;
            department.inactivatedDatetime = DEPARTMENT1.INACTIVATED_DATETIME;
            jidbc.insert(department);
            newDepartmentId = department.departmentId;

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

        // Re-open the database connection and check the inserted row
        retrieveFromDatabaseAndAssertAttributeValuesAreEqualToDepartment1(newDepartmentId);

    }

    @Test
    public void get() {

        // Open a separate database connection and insert a test row
        Department newDepartment = DEPARTMENT1.newInstance();
        Jidbc.insert(DATA_SOURCE, newDepartment);

        // Open a different  database connection and check the inserted row
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = jidbc.get(Department.class, newDepartment.departmentId);
            assertEquals("department.department_abbr", DEPARTMENT1.DEPARTMENT_ABBR, department.departmentAbbr);
            assertEquals("department.department_type", DEPARTMENT1.DEPARTMENT_TYPE, department.departmentType);
            assertEquals("department.department_name", DEPARTMENT1.DEPARTMENT_NAME, department.departmentName);
            assertEquals("department.creation_anniversary", DEPARTMENT1.CREATION_ANNIVERSARY, department.creationAnniversary);
            assertEquals("department.thingy_type", DEPARTMENT1.THINGY_TYPE, department.thingyType);
            assertEquals("department.thingy_number", DEPARTMENT1.THINGY_NUMBER, department.thingyNumber);
            assertEquals("department.another_thingy", DEPARTMENT1.ANOTHER_THINGY, department.anotherThingy);
            assertEquals("department.inactivated_datetime", DEPARTMENT1.INACTIVATED_DATETIME, department.inactivatedDatetime);

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

    }

    @Test
    public void get_gettersAndSetters() {

        // Open a separate database connection and insert a test row
        GetterSetterDepartment newDepartment = DEPARTMENT1.newGetterSetterInstance();
        Jidbc.insert(DATA_SOURCE, newDepartment);

        // Open a different  database connection and check the inserted row
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            GetterSetterDepartment department = jidbc.get(GetterSetterDepartment.class, newDepartment.getDepartmentId());
            assertEquals("department.department_abbr", DEPARTMENT1.DEPARTMENT_ABBR, department.getDepartmentAbbr());
            assertEquals("department.department_type", DEPARTMENT1.DEPARTMENT_TYPE, department.getDepartmentType());
            assertEquals("department.department_name", DEPARTMENT1.DEPARTMENT_NAME, department.getDepartmentName());
            assertEquals("department.creation_anniversary", DEPARTMENT1.CREATION_ANNIVERSARY, department.getCreationAnniversary());
            assertEquals("department.thingy_type", DEPARTMENT1.THINGY_TYPE, department.getThingyType());
            assertEquals("department.thingy_number", DEPARTMENT1.THINGY_NUMBER, department.getThingyNumber());
            assertEquals("department.another_thingy", DEPARTMENT1.ANOTHER_THINGY, department.getAnotherThingy());
            assertEquals("department.inactivated_datetime", DEPARTMENT1.INACTIVATED_DATETIME, department.getInactivatedDatetime());

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

    }

    @Test
    public void getIfExists() {

        // Open a separate database connection and insert a test row
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Open a different  database connection and check the inserted row is found
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            assertNotNull(String.format("Expected Department:%d to exist", DEPARTMENT3.DEPARTMENT_ID), jidbc.getIfExists(Department.class, DEPARTMENT3.DEPARTMENT_ID));

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

        // Open another database connection and check non-existent row is not found
        JidbcConnection jidbc2 = JidbcConnection.from(DATA_SOURCE);
        try {

            Long departmentId = 575757l;
            assertNull(String.format("Expected Department:%d not to exist", departmentId), jidbc2.getIfExists(Department.class, departmentId));

            jidbc2.commitAndClose();
        } catch (Throwable t) {
            throw jidbc2.rollbackCloseAndWrap(t);
        }

    }

    @Test
    public void update() {

        // Open a separate database connection and insert test rows
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());

        // Re-connect to the database and fetch the row and update it
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = jidbc.get(Department.class, DEPARTMENT3.DEPARTMENT_ID);
            department.departmentAbbr = "Zac";
            department.departmentName = "Zac's Department";
            department.inactivatedDatetime = new GregorianCalendar(2009, 8, 14).getTime();
            jidbc.update(department);

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }


        // Re-connect to the database and check the updated row and that at least one other row has not been updated
        JidbcConnection jidbc2 = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = jidbc2.get(Department.class, DEPARTMENT3.DEPARTMENT_ID);
            assertEquals("department.department_id", DEPARTMENT3.DEPARTMENT_ID, department.departmentId);
            assertEquals("department.department_abbr", "Zac", department.departmentAbbr);
            assertEquals("department.department_name", "Zac's Department", department.departmentName);
            assertEquals("department.inactivated_datetime", new GregorianCalendar(2009, 8, 14).getTime(), department.inactivatedDatetime);

            Department department2 = jidbc2.get(Department.class, DEPARTMENT2.DEPARTMENT_ID);
            assertEquals("department.department_id", DEPARTMENT2.DEPARTMENT_ID, department2.departmentId);
            assertEquals("department.department_abbr", DEPARTMENT2.DEPARTMENT_ABBR, department2.departmentAbbr);
            assertEquals("department.department_name", DEPARTMENT2.DEPARTMENT_NAME, department2.departmentName);
            assertEquals("department.inactivated_datetime", DEPARTMENT2.INACTIVATED_DATETIME, department2.inactivatedDatetime);

            jidbc2.commitAndClose();
        } catch (Throwable t) {
            throw jidbc2.rollbackCloseAndWrap(t);
        }

    }

    @Test
    public void save_update() {

        // Open a separate database connection and insert test rows
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());
        Jidbc.insert(DATA_SOURCE, DEPARTMENT2.newInstance());

        // Re-connect to the database and fetch the row then change and save it
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = jidbc.get(Department.class, DEPARTMENT3.DEPARTMENT_ID);
            department.departmentAbbr = "Zac";
            department.departmentName = "Zac's Department";
            department.inactivatedDatetime = new GregorianCalendar(2009, 8, 14).getTime();
            jidbc.save(department);

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }


        // Re-connect to the database and check the updated row and that at least one other row has not been updated
        JidbcConnection jidbc2 = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = jidbc2.get(Department.class, DEPARTMENT3.DEPARTMENT_ID);
            assertEquals("department.department_id", DEPARTMENT3.DEPARTMENT_ID, department.departmentId);
            assertEquals("department.department_abbr", "Zac", department.departmentAbbr);
            assertEquals("department.department_name", "Zac's Department", department.departmentName);
            assertEquals("department.inactivated_datetime", new GregorianCalendar(2009, 8, 14).getTime(), department.inactivatedDatetime);

            Department department2 = jidbc2.get(Department.class, DEPARTMENT2.DEPARTMENT_ID);
            assertEquals("department.department_id", DEPARTMENT2.DEPARTMENT_ID, department2.departmentId);
            assertEquals("department.department_abbr", DEPARTMENT2.DEPARTMENT_ABBR, department2.departmentAbbr);
            assertEquals("department.department_name", DEPARTMENT2.DEPARTMENT_NAME, department2.departmentName);
            assertEquals("department.inactivated_datetime", DEPARTMENT2.INACTIVATED_DATETIME, department2.inactivatedDatetime);

            jidbc2.commitAndClose();
        } catch (Throwable t) {
            throw jidbc2.rollbackCloseAndWrap(t);
        }

    }

    @Test
    public void save_insertWithExplicitId() {


        // Open a new database connection and do the insert
        long newDepartmentId;
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = new Department();
            department.departmentId = jidbc.takeSequenceNextVal("common_id_sequence");
            department.departmentType = DEPARTMENT1.DEPARTMENT_TYPE;
            department.departmentAbbr = DEPARTMENT1.DEPARTMENT_ABBR;
            department.departmentName = DEPARTMENT1.DEPARTMENT_NAME;
            department.creationAnniversary = DEPARTMENT1.CREATION_ANNIVERSARY;
            department.someMonth = DEPARTMENT1.SOME_MONTH;
            department.thingyType = DEPARTMENT1.THINGY_TYPE;
            department.thingyNumber = DEPARTMENT1.THINGY_NUMBER;
            department.anotherThingy = DEPARTMENT1.ANOTHER_THINGY;
            department.inactivatedDatetime = DEPARTMENT1.INACTIVATED_DATETIME;
            jidbc.save(department);
            newDepartmentId = department.departmentId;

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

        // Re-open the database connection and check the inserted row
        retrieveFromDatabaseAndAssertAttributeValuesAreEqualToDepartment1(newDepartmentId);

    }

    @Test
    public void save_insertWithExplicitId_gettersAndSetters() {


        // Open a new database connection and do the insert
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            GetterSetterDepartment department = new GetterSetterDepartment();
            department.setDepartmentId(4242l);
            department.setDepartmentType(DEPARTMENT1.DEPARTMENT_TYPE);
            department.setDepartmentAbbr(DEPARTMENT1.DEPARTMENT_ABBR);
            department.setDepartmentName(DEPARTMENT1.DEPARTMENT_NAME);
            department.setCreationAnniversary(DEPARTMENT1.CREATION_ANNIVERSARY);
            department.setSomeMonth(DEPARTMENT1.SOME_MONTH);
            department.setThingyType(DEPARTMENT1.THINGY_TYPE);
            department.setThingyNumber(DEPARTMENT1.THINGY_NUMBER);
            department.setAnotherThingy(DEPARTMENT1.ANOTHER_THINGY);
            department.setInactivatedDatetime(DEPARTMENT1.INACTIVATED_DATETIME);
            jidbc.save(department);

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

        // Re-open the database connection and check the inserted row
        retrieveFromDatabaseAndAssertAttributeValuesAreEqualToGetterSetterDepartment1(4242l);

    }
    @Test
    public void save_insertWithAutoGeneratedId() {


        // Open a new database connection and do the insert
        long newDepartmentId;
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = new Department();
            department.departmentType = DEPARTMENT1.DEPARTMENT_TYPE;
            department.departmentAbbr = DEPARTMENT1.DEPARTMENT_ABBR;
            department.departmentName = DEPARTMENT1.DEPARTMENT_NAME;
            department.creationAnniversary = DEPARTMENT1.CREATION_ANNIVERSARY;
            department.someMonth = DEPARTMENT1.SOME_MONTH;
            department.thingyType = DEPARTMENT1.THINGY_TYPE;
            department.thingyNumber = DEPARTMENT1.THINGY_NUMBER;
            department.anotherThingy = DEPARTMENT1.ANOTHER_THINGY;
            department.inactivatedDatetime = DEPARTMENT1.INACTIVATED_DATETIME;
            jidbc.save(department);
            newDepartmentId = department.departmentId;

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

        // Re-open the database connection and check the inserted row
        retrieveFromDatabaseAndAssertAttributeValuesAreEqualToDepartment1(newDepartmentId);

    }

    @Test
    public void delete() {

        // Open a separate database connection and insert a test row
        Jidbc.insert(DATA_SOURCE, DEPARTMENT3.newInstance());

        // Re-open the database and verify the row is present
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = jidbc.get(Department.class, DEPARTMENT3.DEPARTMENT_ID);

            jidbc.commitAndClose();
        } catch (Throwable t) {
            throw jidbc.rollbackCloseAndWrap(t);
        }

        // Re-open the database and perform the deletion
        JidbcConnection jidbc2 = JidbcConnection.from(DATA_SOURCE);
        try {

            Department idOnlyDepartment = new Department();
            idOnlyDepartment.departmentId = DEPARTMENT3.DEPARTMENT_ID;
            jidbc2.delete(idOnlyDepartment);

            jidbc2.commitAndClose();
        } catch (Throwable t) {
            throw jidbc2.rollbackCloseAndWrap(t);
        }

        // Re-open the database and verify the row is absent
        JidbcConnection jidbc3 = JidbcConnection.from(DATA_SOURCE);
        try {

            assertNull(String.format("Department:%d does not appear to have been deleted", DEPARTMENT3.DEPARTMENT_ID), jidbc3.getIfExists(Department.class, DEPARTMENT3.DEPARTMENT_ID));

            jidbc3.commitAndClose();
        } catch (Throwable t) {
            throw jidbc3.rollbackCloseAndWrap(t);
        }

        // Re-open the database, attempt to delete a non-existent row and check exception
        JidbcConnection jidbc4 = JidbcConnection.from(DATA_SOURCE);
        try {

            Department idOnlyDepartment = new Department();
            idOnlyDepartment.departmentId = 575757l;
            try {
                jidbc4.delete(idOnlyDepartment);
                fail("Expected NotFoundRuntimeException");
            } catch (NotFoundRuntimeException e) {
            }

            jidbc4.commitAndClose();
        } catch (Throwable t) {
            throw jidbc4.rollbackCloseAndWrap(t);
        }

    }

}
