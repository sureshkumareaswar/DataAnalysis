package org.innoboon.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.innoboon.model.response.ResponseData;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility class for performing operations on a list of files, such as calculating the sum, average, or filtering based on prefixes or suffixes.
 */
public class FileOperations {
    /**
     * Calculates the sum of numeric values in the specified column from CSV and JSON files.
     *
     * @param files      The list of files to process.
     * @param columnName The name of the column to perform the sum operation.
     * @return A list of ResponseData objects containing the column name and its corresponding sum.
     * @throws IllegalArgumentException If an unsupported file type is encountered or if there is an issue with the file content.
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
     * Handles the sum operation for a CSV file.
     *
     * @param file       The CSV file to process.
     * @param columnName The name of the column to perform the sum operation.
     * @return A ResponseData object containing the column name and its corresponding sum.
     * @throws IllegalArgumentException If there is an issue with the file content or if the column is not found.
     */
    @SneakyThrows
    private static ResponseData handleCSVFileSum(File file, String columnName) {
        try (CSVParser parser = CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader())) {
            int columnIndex = findColumnIndex(parser, columnName);
            double sum = parser.getRecords().stream()
                    .map(record -> record.get(columnIndex))
                    .filter(value -> !value.isEmpty())
                    .mapToDouble(value -> {
                        try {
                            return Double.parseDouble(value);
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Invalid value for column " + columnName + ": " + value, e);
                        }
                    })
                    .sum();
            return new ResponseData(columnName, sum);
        }
    }

    /**
     * Handles the sum operation for a JSON file.
     *
     * @param file       The JSON file to process.
     * @param columnName The name of the column to perform the sum operation.
     * @return A ResponseData object containing the column name and its corresponding sum.
     * @throws IllegalArgumentException If there is an issue with the file content or if the column is not found.
     */
    @SneakyThrows
    private static ResponseData handleJSONFileSum(File file, String columnName) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<String, Object>> records = objectMapper.readValue(file, new TypeReference<>() {
        });
        if (columnName != null && !records.get(0).containsKey(columnName)) {
            throw new IllegalArgumentException("Column not found in JSON file: " + columnName);
        }
        double sum = records.stream()
                .map(record -> record.get(columnName))
                .filter(value -> value != null && !value.toString().isEmpty())
                .mapToDouble(value -> {
                            try {
                                return Double.parseDouble(value.toString());
                            } catch (NumberFormatException e) {
                                throw new IllegalArgumentException("Invalid value for column " + columnName + ": " + value, e);
                            }
                        }
                )
                .sum();
        return new ResponseData(columnName, sum);
    }

    /**
     * Calculates the average of numeric values in the specified column from CSV and JSON files.
     *
     * @param files      The list of files to process.
     * @param columnName The name of the column to perform the average operation.
     * @return A map containing the column name and its corresponding average.
     * @throws IllegalArgumentException If an unsupported file type is encountered or if there is an issue with the file content.
     */
    @SneakyThrows
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
     * Handles the average operation for a CSV file.
     *
     * @param file       The CSV file to process.
     * @param columnName The name of the column to perform the average operation.
     * @param resultMap  The map to store the result of the average operation.
     * @throws IllegalArgumentException If there is an issue with the file content or if the column is not found.
     */
    @SneakyThrows
    private static void handleCSVFileForAverage(File file, String columnName, Map<String, Double> resultMap) {
        List<String> lines = Files.readAllLines(file.toPath());
        // Check if the column exists in the header
        int columnIndex = getHeaderIndex(lines.get(0).split(","), columnName);
        double avg = lines.stream().skip(1)
                .map(line -> line.split(","))
                .filter(parts -> parts.length > columnIndex)
                .mapToDouble(parts -> Double.parseDouble(parts[columnIndex].trim()))
                .average()
                .orElseThrow(() -> new IllegalArgumentException("No valid data in the specified column in file: " + file.getName()));
        resultMap.put(columnName, avg);
    }

    /**
     * Handles the average operation for a JSON file.
     *
     * @param file       The JSON file to process.
     * @param columnName The name of the column to perform the average operation.
     * @param resultMap  The map to store the result of the average operation.
     * @throws IllegalArgumentException If there is an issue with the file content or if the column is not found.
     */
    @SneakyThrows
    private static void handleJSONFileForAverage(File file, String columnName, Map<String, Double> resultMap) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<Map<String, Object>> records = objectMapper.readValue(file, new TypeReference<>() {
        });
        if (columnName != null && !records.get(0).containsKey(columnName)) {
            throw new IllegalArgumentException("Column not found in JSON file: " + columnName);
        }
        double avg = records.stream()
                .map(record -> record.get(columnName))
                .filter(value -> value != null && !value.toString().isEmpty())
                .mapToDouble(value -> Double.parseDouble(value.toString()))
                .average()
                .orElseThrow(() -> new IllegalArgumentException("No valid data in the specified column in file: " + file.getName()));
        resultMap.put(columnName, avg);
    }

    /**
     * Filters values in the specified column based on a prefix from CSV and JSON files.
     *
     * @param files      The list of files to process.
     * @param columnName The name of the column to filter.
     * @param prefix     The prefix to filter values.
     * @return A map containing the column name and a list of filtered values.
     * @throws IllegalArgumentException If an unsupported file type is encountered or if there is an issue with the file content.
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
     * Handles the startsWith operation for a CSV file.
     *
     * @param file       The CSV file to process.
     * @param columnName The name of the column to filter.
     * @param prefix     The prefix to filter values.
     * @return A map containing the column name and a list of filtered values.
     * @throws IllegalArgumentException If there is an issue with the file content or if the column is not found.
     */
    @SneakyThrows
    private static Map<String, List<String>> handleCsvFileForStartWith(File file, String columnName, String prefix) {
        Map<String, List<String>> resultMap = new HashMap<>();
        List<String> lines = Files.readAllLines(file.toPath());
        int columnIndex = getHeaderIndex(lines.get(0).split(","), columnName);
        List<String> result = new ArrayList<>();
        boolean foundPrefix = false;
        for (String line : lines.subList(1, lines.size())) {
            String[] parts = line.split(",");
            if (parts.length > columnIndex) {
                String value = parts[columnIndex].trim();
                if (foundPrefix) {
                    if (value.startsWith(prefix)) {
                        result.add(value);
                    }
                } else if (value.startsWith(prefix)) {
                    foundPrefix = true;
                    result.add(value);
                }
            }
        }
        resultMap.put(columnName, result);
        return resultMap;
    }

    /**
     * Handles the startsWith operation for a JSON file.
     *
     * @param file       The JSON file to process.
     * @param columnName The name of the column to filter.
     * @param prefix     The prefix to filter values.
     * @return A map containing the column name and a list of filtered values.
     * @throws IllegalArgumentException If there is an issue with the file content or if the column is not found.
     */
    @SneakyThrows
    private static Map<String, List<String>> handleJsonFileStartWith(File file, String columnName, String prefix) {
        try {
            String jsonString = Files.readString(file.toPath(), StandardCharsets.UTF_8);
            Object json = sneakyThrow(() -> new JSONTokener(jsonString).nextValue());
            Map<String, List<String>> resultMap = new HashMap<>();
            if (json instanceof JSONObject) {
                handleJsonObjectStart((JSONObject) json, columnName, prefix, resultMap);
            } else if (json instanceof JSONArray) {
                handleJsonArrayStart((JSONArray) json, columnName, prefix, resultMap);
            } else {
                throw new IllegalArgumentException("Unsupported JSON format");
            }
            // Check if the specified prefix exists in the result map
            if (resultMap.containsKey(columnName)) {
                List<String> values = resultMap.get(columnName);
                if (prefix != null) {
                    values = values.stream()
                            .filter(value -> value.startsWith(prefix))
                            .collect(Collectors.toList());
                }
                resultMap.put(columnName, values);
            }
            return resultMap;
        } catch (IOException e) {
            throw new IllegalArgumentException("Error reading file: " + e.getMessage(), e);
        }
    }

    /**
     * Filters values in the specified column based on a suffix from CSV and JSON files.
     *
     * @param files      The list of files to process.
     * @param columnName The name of the column to filter.
     * @param suffix     The suffix to filter values.
     * @return A map containing the column name and a list of filtered values.
     * @throws IllegalArgumentException If an unsupported file type is encountered or if there is an issue with the file content.
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
     * Handles the endsWith operation for a CSV file.
     *
     * @param file       The CSV file to process.
     * @param resultMap  The map to store the result of the endsWith operation.
     * @param columnName The name of the column to filter.
     * @param suffix     The suffix to filter values.
     * @throws IllegalArgumentException If there is an issue with the file content or if the column is not found.
     */
    @SneakyThrows(IOException.class)
    private static void handleCsvFileEndWith(File file, Map<String, List<String>> resultMap, String columnName, String suffix) {
        List<String> lines = Files.readAllLines(file.toPath());
        // Check if the column exists in the header
        int columnIndex = getHeaderIndex(lines.get(0).split(","), columnName);
        List<String> result = lines.stream().skip(1) // Skip the header row
                .map(line -> line.split(","))
                .filter(parts -> parts.length > columnIndex)
                .filter(parts -> parts[columnIndex].trim().endsWith(suffix))
                .map(parts -> parts[columnIndex].trim())
                .collect(Collectors.toList());
        resultMap.put(columnName, result);
    }

    /**
     * Handles the endsWith operation for a JSON file.
     *
     * @param file       The JSON file to process.
     * @param resultMap  The map to store the result of the endsWith operation.
     * @param columnName The name of the column to filter.
     * @param suffix     The suffix to filter values.
     * @throws IllegalArgumentException If there is an issue with the file content or if the column is not found.
     */
    @SneakyThrows
    private static void handleJsonFileEndWith(File file, Map<String, List<String>> resultMap, String columnName, String suffix) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String jsonString = reader.lines().collect(Collectors.joining());
            Object json = new JSONTokener(jsonString).nextValue();
            if (json instanceof JSONObject) {
                handleJsonObjectEndWith((JSONObject) json, columnName, suffix, resultMap);
            } else if (json instanceof JSONArray) {
                handleJsonArrayEnd((JSONArray) json, columnName, suffix, resultMap);
            } else {
                throw new IllegalArgumentException("Invalid JSON file format in: " + file.getName());
            }
        }
    }

    // Helper methods...
    @SneakyThrows
    private static void handleJsonObjectStart(JSONObject jsonObject, String columnName, String prefix, Map<String, List<String>> resultMap) {
        // Handle the case when the JSON structure is a single object
        if (jsonObject.has(columnName)) {
            String value = jsonObject.getString(columnName);
            if (prefix.isEmpty() || value.startsWith(prefix)) {
                resultMap.computeIfAbsent(columnName, k -> new LinkedList<>()).add(value);
            }
        } else {
            throw new IllegalArgumentException("Column not found in JSON file: " + columnName);
        }
    }

    @SneakyThrows
    private static void handleJsonArrayStart(JSONArray jsonArray, String columnName, String prefix, Map<String, List<String>> resultMap) {
        // Handle the case when the JSON structure is an array of objects
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            handleJsonObjectStart(obj, columnName, prefix, resultMap);
        }
    }

    @SneakyThrows
    private static void handleJsonObjectEndWith(JSONObject jsonObject, String columnName, String suffix, Map<String, List<String>> resultMap) {
        if (jsonObject.has(columnName)) {
            String value = jsonObject.getString(columnName);
            if (suffix.isEmpty() || value.endsWith(suffix)) {
                resultMap.computeIfAbsent(columnName, k -> new LinkedList<>()).add(value);
            }
        } else {
            throw new IllegalArgumentException("Column not found in JSON file: " + columnName);
        }
    }

    @SneakyThrows
    private static void handleJsonArrayEnd(JSONArray jsonArray, String columnName, String suffix, Map<String, List<String>> resultMap) {
        // Handle the case when the JSON structure is an array of objects
        for (int i = 0; i < jsonArray.length(); i++) {
            JSONObject obj = jsonArray.getJSONObject(i);
            handleJsonObjectEndWith(obj, columnName, suffix, resultMap);
        }
    }

    /**
     * Retrieves a value indicating whether the specified column exists in the header row of a CSV file.
     *
     * @param parser     The CSVParser instance.
     * @param columnName The name of the column to find.
     * @return The index of the column in the CSV file.
     * @throws IllegalArgumentException If the CSVParser is null, if the CSV file has no header, or if the column is not found.
     */
    private static int findColumnIndex(CSVParser parser, String columnName) {
        Optional.ofNullable(parser)
                .orElseThrow(() -> new IllegalArgumentException("CSVParser cannot be null"));
        Map<String, Integer> headerMap = Optional.ofNullable(parser.getHeaderMap())
                .orElseThrow(() -> new IllegalStateException("CSV file does not have a header"));
        return Optional.ofNullable(headerMap.get(columnName))
                .orElseThrow(() -> new IllegalArgumentException("Column not found : " + columnName));
    }

    /**
     * Finds the index of a column in the CSV header.
     *
     * @param headers    The array of header values.
     * @param columnName The name of the column to find.
     * @return The index of the column in the CSV header.
     * @throws IllegalArgumentException If the column is not found in the CSV header.
     */
    private static int getHeaderIndex(String[] headers, String columnName) {
        return IntStream.range(0, headers.length)
                .filter(i -> headers[i].trim().equalsIgnoreCase(columnName.trim()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Column '" + columnName + "' not found in the CSV file "));
    }

    /**
     * Executes a callable operation and wraps any checked exceptions into a runtime exception.
     *
     * @param callable The callable operation to execute.
     * @param <T>      The type of the result.
     * @return The result of the callable operation.
     * @throws RuntimeException If the callable operation throws a checked exception.
     */
    public static <T> T sneakyThrow(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}