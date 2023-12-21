package org.innoboon.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.example.model.ResponseData;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

/**
 * FileUtility class for processing CSV or JSON files and performing calculations.
 */
public class FileUtility {
    /**
     * Calculates the sum of a specified column in each CSV or JSON file in the given list.
     *
     * @param files      The list of CSV files.
     * @param columnName The name of the column for which the sum is calculated.
     * @return A list of ResponseData objects containing the column name and the sum for each file.
     * @throws IllegalArgumentException If an unsupported file type is encountered.
     */
    @SneakyThrows
    public static List<ResponseData> sum(List<File> files, String columnName) {
        List<ResponseData> resultList = new ArrayList<>();
        for (File file : files) {
            if (file.getName().endsWith(".csv")) {
                resultList.add(handleCSVFileSum(file, columnName));
            } else if (file.getName().endsWith(".json")) {
                resultList.add(handleJSONFileSum(file, columnName));
            } else {
                throw new IllegalArgumentException("Unsupported file type: " + file.getName());
            }
        }
        return resultList;
    }

    /**
     * Handles the calculation of the sum for a specified column in a CSV file.
     */
    @SneakyThrows
    private static ResponseData handleCSVFileSum(File file, String columnName) {
        try (CSVParser parser = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader())) {
            int columnIndex = findColumnIndex(parser, columnName);
            if (columnIndex == -1) {
                throw new IllegalArgumentException("Column not found: " + columnName);
            }
            double sum = parser.getRecords().stream()
                    .map(record -> record.get(columnIndex))
                    .filter(value -> !value.isEmpty())
                    .mapToDouble(Double::parseDouble)
                    .sum();
            return new ResponseData(columnName, sum);
        }
    }

    /**
     * Handles the calculation of the sum for a specified column in a JSON file.
     */
    @SneakyThrows
    private static ResponseData handleJSONFileSum(File file, String columnName) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<String, Object>> records = objectMapper.readValue(file, new TypeReference<>() {
        });
        double sum = records.stream()
                .map(record -> record.get(columnName))
                .filter(value -> value != null && !value.toString().isEmpty())
                .mapToDouble(value -> Double.parseDouble(value.toString()))
                .sum();
        return new ResponseData(columnName, sum);
    }

    /**
     * Calculates the average of a specified column in each CSV or JSON file in the given list.
     *
     * @param files      The list of CSV files.
     * @param columnName The name of the column for which the average is calculated.
     * @return A map containing the column name and the average for each file.
     * @throws IllegalArgumentException If an unsupported file type is encountered.
     */
    public static Map<String, Double> average(List<File> files, String columnName) {
        Map<String, Double> resultMap = new HashMap<>();
        for (File file : files) {
            if (file.getName().endsWith(".csv")) {
                handleCSVFileForAverage(file, columnName, resultMap);
            } else if (file.getName().endsWith(".json")) {
                handleJSONFileForAverage(file, columnName, resultMap);
            } else {
                throw new IllegalArgumentException("Unsupported file type: " + file.getName());
            }
        }
        return resultMap;
    }

    /**
     * Handles the calculation of the average for a specified column in a CSV file.
     */
    @SneakyThrows
    private static void handleCSVFileForAverage(File file, String columnName, Map<String, Double> resultMap) {
        List<String> lines = Files.readAllLines(file.toPath());
        int columnIndex = getHeaderIndex(lines.get(0).split(","), columnName);
        // Check if the column exists in the header
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column '" + columnName + "' not found in the CSV file: " + file.getName());
        }
        double avg = lines.stream().skip(1)
                .map(line -> line.split(","))
                .filter(parts -> parts.length > columnIndex)
                .mapToDouble(parts -> Double.parseDouble(parts[columnIndex].trim()))
                .average()
                .orElseThrow(() -> new IllegalArgumentException("No valid data in the specified column in file: " + file.getName()));
        resultMap.put(columnName, avg);
    }

    /**
     * Handles the calculation of the average for a specified column in a JSON file.
     */
    @SneakyThrows
    private static void handleJSONFileForAverage(File file, String columnName, Map<String, Double> resultMap) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<String, Object>> records = objectMapper.readValue(file, new TypeReference<>() {
        });
        double avg = records.stream()
                .map(record -> record.get(columnName))
                .filter(value -> value != null && !value.toString().isEmpty())
                .mapToDouble(value -> Double.parseDouble(value.toString()))
                .average()
                .orElseThrow(() -> new IllegalArgumentException("No valid data in the specified column in file: " + file.getName()));
        resultMap.put(columnName, avg);
    }

    /**
     * Finds values in a specified column of CSV or JSON files that start with a given prefix.
     *
     * @param files      The list of files (CSV or JSON) to process.
     * @param columnName The name of the column to search for values.
     * @param prefix     The prefix to check for in the values.
     * @return A map containing the column name and a list of values starting with the specified prefix for each CSV file.
     * @throws IllegalArgumentException If an unsupported file format is encountered.
     */
    @SneakyThrows
    public static Map<String, List<String>> startsWith(List<File> files, String columnName, String prefix) {
        Map<String, List<String>> resultMap = new HashMap<>();
        for (File file : files) {
            if (file.getName().toLowerCase().endsWith(".csv")) {
                resultMap.putAll(handleCsvFileForStartWith(file, columnName, prefix));
            } else if (file.getName().toLowerCase().endsWith(".json")) {
                resultMap.putAll(handleJsonFileStartWith(file, columnName, prefix));
            } else {
                throw new IllegalArgumentException("Unsupported file format: " + file.getName());
            }
        }
        return resultMap;
    }

    /**
     * Handles the search for values starting with a given prefix in a CSV file.
     */
    @SneakyThrows
    private static Map<String, List<String>> handleCsvFileForStartWith(File file, String columnName, String prefix) {
        Map<String, List<String>> resultMap = new HashMap<>();
        List<String> lines = Files.readAllLines(file.toPath());
        int columnIndex = getHeaderIndex(lines.get(0).split(","), columnName);
        // Check if the column exists in the header
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column '" + columnName + "' not found in the CSV file: " + file.getName());
        }
        final boolean[] foundPrefix = {false};
        List<String> result = lines.stream().skip(1) // Skip the header row
                .map(line -> line.split(",")).filter(parts -> parts.length > columnIndex).filter(parts -> {
                    // Use a flag to track whether the prefix has been found
                    if (foundPrefix[0]) {
                        return false;  // Skip values until the next occurrence after the prefix
                    }
                    boolean startsWith = parts[columnIndex].trim().startsWith(prefix);
                    foundPrefix[0] = startsWith;
                    return startsWith;
                }).map(parts -> parts[columnIndex].trim()).collect(Collectors.toList());
        resultMap.put(columnName, result);
        return resultMap;
    }

    /**
     * Handles the search for values starting with a given prefix in a JSON file.
     */
    @SneakyThrows
    private static Map<String, List<String>> handleJsonFileStartWith(File file, String columnName, String prefix) {
        Map<String, List<String>> resultMap = new HashMap<>();
        try (FileReader fileReader = new FileReader(file)) {
            // Read the contents of the file into a string
            StringBuilder jsonString = new StringBuilder();
            int character;
            while ((character = fileReader.read()) != -1) {
                jsonString.append((char) character);
            }
            // Parse the JSON file contents
            Object json = new JSONTokener(jsonString.toString()).nextValue();
            if (json instanceof JSONObject) {
                handleJsonObjectStart((JSONObject) json, columnName, prefix, resultMap);
            } else if (json instanceof JSONArray) {
                handleJsonArrayStart((JSONArray) json, columnName, prefix, resultMap);
            }
        }
        return resultMap;
    }

    /**
     * Handles the search for values starting with a given prefix in a JSON object.
     */
    @SneakyThrows
    private static void handleJsonObjectStart(JSONObject jsonObject, String columnName, String prefix, Map<String, List<String>> resultMap) {
        // Handle the case when the JSON structure is a single object
        if (jsonObject.has(columnName)) {
            String value = jsonObject.getString(columnName);
            if (value.startsWith(prefix)) {
                resultMap.computeIfAbsent(columnName, k -> new LinkedList<>()).add(value);
            }
        }
    }

    /**
     * Handles the search for values starting with a given prefix in a JSON array.
     */
    @SneakyThrows
    private static void handleJsonArrayStart(JSONArray jsonArray, String columnName, String prefix, Map<String, List<String>> resultMap) {
        // Handle the case when the JSON structure is an array of objects
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            handleJsonObjectStart(obj, columnName, prefix, resultMap);
        }
    }

    /**
     * Finds values in a specified column of CSV or JSON files that end with a given suffix.
     *
     * @param files      The list of files (CSV or JSON) to process.
     * @param columnName The name of the column to search for values.
     * @param suffix     The suffix to check for in the values.
     * @return A map containing the column name and a list of values ending with the specified suffix for each file.
     * @throws IllegalArgumentException If an unsupported file format is encountered.
     */
    @SneakyThrows
    public static Map<String, List<String>> endsWith(List<File> files, String columnName, String suffix) {
        Map<String, List<String>> resultMap = new HashMap<>();
        for (File file : files) {
            if (file.getName().toLowerCase().endsWith(".csv")) {
                handleCsvFileEndWith(file, resultMap, columnName, suffix);
            } else if (file.getName().toLowerCase().endsWith(".json")) {
                handleJsonFileEndWith(file, resultMap, columnName, suffix);
            } else {
                throw new IllegalArgumentException("Unsupported file format: " + file.getName());
            }
        }
        return resultMap;
    }

    /**
     * Handles the search for values ending with a given suffix in a CSV file.
     */
    @SneakyThrows
    private static void handleCsvFileEndWith(File file, Map<String, List<String>> resultMap, String columnName, String suffix) {
        List<String> lines = Files.readAllLines(file.toPath());
        int columnIndex = getHeaderIndex(lines.get(0).split(","), columnName);
        // Check if the column exists in the header
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column '" + columnName + "' not found in the CSV file");
        }
        final boolean[] foundSuffix = {false};
        List<String> result = lines.stream().skip(1) // Skip the header row
                .map(line -> line.split(","))
                .filter(parts -> parts.length > columnIndex)
                .filter(parts -> {
                    if (foundSuffix[0]) {
                        return false;  // Skip values until the next occurrence after the suffix
                    }
                    boolean endsWith = parts[columnIndex].trim().endsWith(suffix);
                    foundSuffix[0] = endsWith;
                    return endsWith;
                })
                .map(parts -> parts[columnIndex].trim())
                .collect(Collectors.toList());
        resultMap.put(columnName, result);
    }

    /**
     * Handles the search for values ending with a given suffix in a JSON file.
     */
    @SneakyThrows
    private static void handleJsonFileEndWith(File file, Map<String, List<String>> resultMap, String columnName, String suffix) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            // Read the contents of the file into a string
            StringBuilder jsonString = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                jsonString.append(line);
            }
            // Parse the JSON file contents
            Object json = new JSONTokener(jsonString.toString()).nextValue();
            if (json instanceof JSONObject) {
                handleJsonObjectEndWith((JSONObject) json, columnName, suffix, resultMap);
            } else if (json instanceof JSONArray) {
                handleJsonArrayEnd((JSONArray) json, columnName, suffix, resultMap);
            } else {
                throw new IllegalArgumentException("Invalid JSON file format in: " + file.getName());
            }
        }
    }

    /**
     * Handles the search for values ending with a given suffix in a JSON object.
     */
    @SneakyThrows
    private static void handleJsonObjectEndWith(JSONObject jsonObject, String columnName, String suffix, Map<String, List<String>> resultMap) {
        // Handle the case when the JSON structure is a single object
        if (jsonObject.has(columnName)) {
            String value = jsonObject.getString(columnName);
            if (value.endsWith(suffix)) {
                resultMap.computeIfAbsent(columnName, k -> new LinkedList<>()).add(value);
            }
        }
    }

    /**
     * Handles the search for values ending with a given suffix in a JSON array.
     */
    @SneakyThrows
    private static void handleJsonArrayEnd(JSONArray jsonArray, String columnName, String suffix, Map<String, List<String>> resultMap) {
        // Handle the case when the JSON structure is an array of objects
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            handleJsonObjectEndWith(obj, columnName, suffix, resultMap);
        }
    }

    private static int findColumnIndex(CSVParser parser, String columnName) {
        return parser.getHeaderMap().get(columnName);
    }

    private static int getHeaderIndex(String[] headers, String columnName) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].trim().equalsIgnoreCase(columnName.trim())) {
                return i;
            }
        }
        return -1;
    }
}