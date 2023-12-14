package org.example;


import org.example.utils.FileUtility;

import java.io.IOException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        String filePath = "/home/ib-38/Downloads/student-dataset.csv";
        String columnName = "mark";
        double sum = FileUtility.sum(filePath, columnName);
        double average = FileUtility.average(filePath, columnName);
        List<String> startWIth = FileUtility.startsWith(filePath, columnName, "Natasha Yarusso");
        List<String> endWith = FileUtility.endsWith(filePath, columnName, "David Weber");
        System.out.println("Sum: " + sum);
        System.out.println("avg: " + average);
        System.out.println("startWith: " + startWIth);
        System.out.println("endWith: " + endWith);

    }
}