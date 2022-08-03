package com.demo;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import com.demo.entity.Student;


public class AerospikeClientDemo {

    public static final String HOST = "127.0.0.1";
    public static final int PORT = 3000;
    public static final String NAMESPACE = "xiaolong";
    public static final String SET = "student";
    public static final String FIELD_NAME = "name";
    public static final String FIELD_AGE = "age";
    public static final String FIELD_KEY = "__key";

    //connect to local aerospike server started as docker container
    private static AerospikeClient client = new AerospikeClient(HOST, PORT);
    private static WritePolicy policy = new WritePolicy();

    static {
        //TimeOut=100ms
        policy.setTimeout(100);
    }

    public static void main(String[] args) throws Exception {
        Student s1 = new Student(7, "wangwu", 31);
        saveStudent(s1);

        Student s2 = findStudent(7);
        System.out.println(s2);

        deleteStudent(7);

        client.close();
    }

    private static void saveStudent(Student s) {
        //create a PK for a record.
        Key key = new Key(NAMESPACE, SET, s.getId());
        //fill other fields for this record.
        Bin bin1 = new Bin(FIELD_NAME, s.getName());
        Bin bin2 = new Bin(FIELD_AGE, s.getAge());

        // save a record
        client.put(policy, key, bin1, bin2);

        // read a record
        Record record = client.get(null, key);
        System.out.println(record);
    }

    private static void deleteStudent(int id) {
        Key key = new Key(NAMESPACE, SET, id);
        boolean exists = client.delete(policy, key);
        if (exists) {
            System.out.println("Key " + id + " is deleted");
        } else {
            System.err.println("Key not exists for " + id);
        }
    }

    private static Student findStudent(int id) {
        Key key = new Key(NAMESPACE, SET, id);
        Record record = client.get(policy, key);
        if (record != null) {
            return new Student(key.userKey.toInteger(),
                    (String)record.bins.get(FIELD_NAME),
                    Integer.parseInt(record.bins.get(FIELD_AGE).toString()));
        } else {
            System.err.println("Key not exists for " + id);
            return null;
        }
    }
}
