package com.demo;

import com.demo.entity.Student;

import java.sql.*;

import static com.demo.AerospikeClientDemo.*;

public class AerospikeJdbcDemo {

    private static final String URL = "jdbc:aerospike:localhost:3000/" + NAMESPACE;
    private static final String DRIVER = "com.aerospike.jdbc.AerospikeDriver";

    private static Connection CONNECTION = null;

    static {
        try {
            Class.forName(DRIVER);
            CONNECTION = DriverManager.getConnection(URL);
        } catch (ClassNotFoundException e) {
            System.err.println("No driver found.");
        } catch (SQLException e2) {
            System.err.println("Get connection error");
            e2.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            Student s1 = new Student(8, "zhaoliu", 80);
            saveStudent(s1);

            Student s2 = findStudent(8);
            System.out.println(s2);

            deleteStudent(8);

            Student s3 = findStudent(8);
            System.out.println("After Delete: " + s3);
        } finally {
            try {
                CONNECTION.close();
            } catch (SQLException e) {
                System.err.println("Close connection error");
            }
        }

    }

    private static void saveStudent(Student s) {
        try {
            String query = String.format("insert into " + SET + " (__key, name, age) values(%s,'%s',%s)",
                    s.getId(), s.getName(), s.getAge());
            boolean success = CONNECTION.createStatement().execute(query);
            if (!success) {
                System.err.println("Execute SQL failed.");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    private static Student findStudent(int id) {
        Student s = null;
        try {
            String query = "select * from " + SET + " where __key = " + id;
            ResultSet resultSet = CONNECTION.createStatement().executeQuery(query);
            while (resultSet.next()) {
                String name = resultSet.getString(FIELD_NAME);
                int age = Integer.parseInt(resultSet.getString(FIELD_AGE));
                int key = Integer.parseInt(resultSet.getString(FIELD_KEY));
                s = new Student(key, name, age);
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        return s;
    }

    private static void deleteStudent(int id) {
        try {
            String query = "delete from " + SET + " where __key = " + id;
            boolean success = CONNECTION.createStatement().execute(query);
            if (!success) {
                System.err.println("Execute SQL failed.");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }
}
