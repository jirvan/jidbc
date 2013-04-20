package com.jirvan.jidbc;


import org.testng.annotations.*;

import java.math.*;
import java.util.*;

import static org.testng.AssertJUnit.*;

public class CRUDTests extends TestsBase {

    private final static String DEPARTMENT1_ABBR = "HR";
    private final static String DEPARTMENT1_NAME = "Human Resources";
    private final static String DEPARTMENT1_THINGY_TYPE = "Strawberry";
    private final static Integer DEPARTMENT1_THINGY_NUMBER = 42;
    private final static BigDecimal DEPARTMENT1_ANOTHER_THINGY = new BigDecimal("42.58");
    private final static Date DEPARTMENT1_INACTIVATED_DATETIME = new GregorianCalendar(2012, 5, 1).getTime();

    private final static long DEPARTMENT2_ID = 42;
    private final static String DEPARTMENT2_ABBR = "HR";
    private final static String DEPARTMENT2_NAME = "Human Resources";
    private final static Date DEPARTMENT2_INACTIVATED_DATETIME = new GregorianCalendar(2012, 5, 3).getTime();

    private final static long DEPARTMENT3_ID = 423636;
    private final static String DEPARTMENT3_ABBR = "TR";
    private final static String DEPARTMENT3_NAME = "Threat Resolution";
    private final static Date DEPARTMENT3_INACTIVATED_DATETIME = new GregorianCalendar(2012, 8, 1).getTime();


    @Test
    public void insert_and_queryFor() {


        // Open a new database connection and do the insert
        long newDepartmentId;
        JidbcConnection jidbc = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = new Department();
            department.departmentId = jidbc.takeSequenceNextVal("common_id_sequence");
            department.departmentAbbr = DEPARTMENT1_ABBR;
            department.departmentName = DEPARTMENT1_NAME;
            department.thingyType = DEPARTMENT1_THINGY_TYPE;
            department.thingyNumber = DEPARTMENT1_THINGY_NUMBER;
            department.anotherThingy = DEPARTMENT1_ANOTHER_THINGY;
            department.inactivatedDatetime = DEPARTMENT1_INACTIVATED_DATETIME;
            jidbc.insert(department);
            newDepartmentId = department.departmentId;

        } finally {
            jidbc.release();
        }

        // Re-open the database connection and check the inserted row
        JidbcConnection jidbc2 = JidbcConnection.from(DATA_SOURCE);
        try {

            Department department = jidbc2.queryFor(Department.class, "where department_id = ?", newDepartmentId);
            assertEquals("department.department_abbr", DEPARTMENT1_ABBR, department.departmentAbbr);
            assertEquals("department.department_name", DEPARTMENT1_NAME, department.departmentName);
            assertEquals("department.thingy_type", DEPARTMENT1_THINGY_TYPE, department.thingyType);
            assertEquals("department.thingy_number", DEPARTMENT1_THINGY_NUMBER, department.thingyNumber);
            assertEquals("department.another_thingy", DEPARTMENT1_ANOTHER_THINGY, department.anotherThingy);
            assertEquals("department.inactivated_datetime", DEPARTMENT1_INACTIVATED_DATETIME, department.inactivatedDatetime);

        } finally {
            jidbc2.release();
        }

    }

//    public void test_insert_explicitPk() {
//
//        // Open a new database connection and do the insert
//        long id;
//        DbConnection conn1 = EnvironmentHelper.openConnectionToTestDatabase();
//        try {
//
//            Department department = new Department();
//            department.departmentId = DEPARTMENT2_ID;
//            department.departmentAbbr = DEPARTMENT2_ABBR;
//            department.departmentName = DEPARTMENT2_NAME;
//            department.thingyType = "Strawberry";
//            department.thingyNumber = 42;
//            department.anotherThingy = BigDecimal.valueOf(42);
//            department.inactivatedDatetime = DEPARTMENT2_INACTIVATED_DATETIME;
//            id = department.insert(conn1);
//
//            // Check the returned ID
//            assertEquals("returned ID", DEPARTMENT2_ID, id);
//
//        } finally {
//            conn1.release();
//        }
//
//        // Re-open the database connection and check the inserted row
//        DbConnection conn2 = EnvironmentHelper.openConnectionToTestDatabase();
//        try {
//
//            Department department = Department.fetch(conn2, id);
//            assertEquals("department.department_abbr", DEPARTMENT2_ABBR, department.departmentAbbr);
//            assertEquals("department.department_name", DEPARTMENT2_NAME, department.departmentName);
//            assertEquals("department.inactivated_datetime", DEPARTMENT2_INACTIVATED_DATETIME, department.inactivatedDatetime);
//
//        } finally {
//            conn2.release();
//        }
//
//    }
//
//    public void test_fetch() {
//
//        // Open a new connection and insert a test row
//        DbConnection conn1 = EnvironmentHelper.openConnectionToTestDatabase();
//        try {
//
//            Department department = new Department();
//            department.departmentId = DEPARTMENT3_ID;
//            department.departmentAbbr = DEPARTMENT3_ABBR;
//            department.departmentName = DEPARTMENT3_NAME;
//            department.thingyType = "Strawberry";
//            department.thingyNumber = 42;
//            department.anotherThingy = BigDecimal.valueOf(42);
//            department.inactivatedDatetime = DEPARTMENT3_INACTIVATED_DATETIME;
//            department.insert(conn1);
//
//        } finally {
//            conn1.release();
//        }
//
//        // Re-connect to the database and test the fetch method
//        DbConnection conn2 = EnvironmentHelper.openConnectionToTestDatabase();
//        try {
//
//            Department department = Department.fetch(conn2, DEPARTMENT3_ID);
//            assertEquals("department.department_id", DEPARTMENT3_ID, (long) department.departmentId);
//            assertEquals("department.department_abbr", DEPARTMENT3_ABBR, department.departmentAbbr);
//            assertEquals("department.department_name", DEPARTMENT3_NAME, department.departmentName);
//            assertEquals("department.inactivated_datetime", DEPARTMENT3_INACTIVATED_DATETIME, department.inactivatedDatetime);
//
//        } finally {
//            conn2.release();
//        }
//
//    }
//
//    public void test_update() {
//
//        // Open a new connection and insert a test row
//        DbConnection conn1 = EnvironmentHelper.openConnectionToTestDatabase();
//        try {
//
//            Department department = new Department();
//            department.departmentId = DEPARTMENT3_ID;
//            department.departmentAbbr = DEPARTMENT3_ABBR;
//            department.departmentName = DEPARTMENT3_NAME;
//            department.thingyType = "Strawberry";
//            department.thingyNumber = 42;
//            department.anotherThingy = BigDecimal.valueOf(42);
//            department.inactivatedDatetime = DEPARTMENT3_INACTIVATED_DATETIME;
//            department.insert(conn1);
//
//        } finally {
//            conn1.release();
//        }
//
//        // Re-connect to the database and fetch the row and update it
//        DbConnection conn2 = EnvironmentHelper.openConnectionToTestDatabase();
//        try {
//
//            Department department = Department.fetch(conn2, DEPARTMENT3_ID);
//            department.departmentAbbr = DEPARTMENT1_ABBR;
//            department.departmentName = DEPARTMENT2_NAME;
//            department.inactivatedDatetime = DEPARTMENT1_INACTIVATED_DATETIME;
//            department.update(conn2);
//
//        } finally {
//            conn2.release();
//        }
//
//
//        // Re-open the database and check the updated row
//        DbConnection conn3 = EnvironmentHelper.openConnectionToTestDatabase();
//        try {
//
//            Department department = Department.fetch(conn3, DEPARTMENT3_ID);
//            assertEquals("department.department_id", (Long) DEPARTMENT3_ID, department.departmentId);
//            assertEquals("department.department_abbr", DEPARTMENT1_ABBR, department.departmentAbbr);
//            assertEquals("department.department_name", DEPARTMENT2_NAME, department.departmentName);
//            assertEquals("department.inactivated_datetime", DEPARTMENT1_INACTIVATED_DATETIME, department.inactivatedDatetime);
//
//        } finally {
//            conn3.release();
//        }
//
//    }
//
//
//    public void test_delete() {
//
//        // Open a new connection and insert a test row
//        DbConnection conn1 = EnvironmentHelper.openConnectionToTestDatabase();
//        try {
//
//            Department department = new Department();
//            department.departmentId = DEPARTMENT3_ID;
//            department.departmentAbbr = DEPARTMENT3_ABBR;
//            department.departmentName = DEPARTMENT3_NAME;
//            department.thingyType = "Strawberry";
//            department.thingyNumber = 42;
//            department.anotherThingy = BigDecimal.valueOf(42);
//            department.inactivatedDatetime = DEPARTMENT3_INACTIVATED_DATETIME;
//            department.insert(conn1);
//
//        } finally {
//            conn1.release();
//        }
//
//        // Re-open the database and verify the row is present
//        DbConnection conn2 = EnvironmentHelper.openConnectionToTestDatabase();
//        try {
//
//            Department department = Department.fetch(conn2, DEPARTMENT3_ID);
//
//        } finally {
//            conn2.release();
//        }
//
//        // Re-open the database and perform the deletion
//        DbConnection conn3 = EnvironmentHelper.openConnectionToTestDatabase();
//        try {
//
//            Department idOnlyDepartment = new Department();
//            idOnlyDepartment.departmentId = DEPARTMENT3_ID;
//            idOnlyDepartment.delete(conn3);
//
//        } finally {
//            conn3.release();
//        }
//
//        // Re-open the database and verify the row is absent
//        DbConnection conn4 = EnvironmentHelper.openConnectionToTestDatabase();
//        try {
//
//            if (Department.fetchIfExists(conn4, DEPARTMENT3_ID) != null) {
//                throw new AssertionError(String.format("Department:%d does not appear to have been deleted", DEPARTMENT3_ID));
//            }
//
//        } finally {
//            conn4.release();
//        }
//
//    }
//
//    protected void tearDown() throws Exception {
//        super.tearDown();
//        DbConnection conn = EnvironmentHelper.openConnectionToTestDatabase();
//        try {
//            conn.executeUpdate("drop table if exists departments_test_table");
//        } finally {
//            conn.release();
//        }
//    }

}
