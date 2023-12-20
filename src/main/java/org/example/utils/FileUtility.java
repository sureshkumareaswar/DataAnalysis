package org.example.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * FileUtility class for processing CSV files and performing calculations.
 */
public class FileUtility {

    /**
     * Calculates the sum of a specified column in each CSV file in the given list.
     *
     * @param files      The list of CSV files.
     * @param columnName The name of the column for which the sum is calculated.
     * @return A map containing the column name and the sum for each CSV file.
     * @throws RuntimeException If an error occurs during file processing.
     */
    public static Map<String, Double> sum(List<File> files, String columnName) {
        try {
            return handleException(() -> {
                Map<String, Double> resultMap = new HashMap<>();

                for (File file : files) {
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

                        resultMap.put(columnName, sum);
                    }
                }

                return resultMap;
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Calculates the average of a specified column in each CSV file in the given list.
     *
     * @param files      The list of CSV files.
     * @param columnName The name of the column for which the average is calculated.
     * @return A map containing the column name and the average for each CSV file.
     * @throws RuntimeException If an error occurs during file processing.
     */
    public static Map<String, Double> average(List<File> files, String columnName) {
        try {
            return handleException(() -> {
                Map<String, Double> resultMap = new HashMap<>();

                for (File file : files) {
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

                return resultMap;
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Finds values in a specified column of each CSV file that start with a given prefix.
     *
     * @param files      The list of CSV files.
     * @param columnName The name of the column to search for values.
     * @param prefix     The prefix to check for in the values.
     * @return A map containing the column name and a list of values starting with the specified prefix for each CSV file.
     */
    public static Map<String, List<String>> startsWith(List<File> files, String columnName, String prefix) {
        try {
            return handleException(() -> {
                Map<String, List<String>> resultMap = new HashMap<>();

                for (File file : files) {
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
                }

                return resultMap;
            });
        } catch (IOException e) {
            handleIOException(e);
            return Collections.singletonMap("error", Collections.singletonList("An error occurred while processing the file."));
        }
    }


    /**
     * Finds values in a specified column of each CSV file that end with a given suffix.
     *
     * @param files      The list of CSV files.
     * @param columnName The name of the column to search for values.
     * @param suffix     The suffix to check for in the values.
     * @return A map containing the column name and a list of values ending with the specified suffix for each CSV file.
     */
    public static Map<String, List<String>> endsWith(List<File> files, String columnName, String suffix) {
        try {
            return handleException(() -> {
                Map<String, List<String>> resultMap = new HashMap<>();

                for (File file : files) {
                    List<String> lines = Files.readAllLines(file.toPath());
                    int columnIndex = getHeaderIndex(lines.get(0).split(","), columnName);

                    // Check if the column exists in the header
                    if (columnIndex == -1) {
                        throw new IllegalArgumentException("Column '" + columnName + "' not found in the CSV file: " + file.getName());
                    }

                    final boolean[] foundSuffix = {false};
                    List<String> result = lines.stream().skip(1) // Skip the header row
                            .map(line -> line.split(",")).filter(parts -> parts.length > columnIndex).filter(parts -> {
                                if (foundSuffix[0]) {
                                    return false;  // Skip values until the next occurrence after the suffix
                                }
                                boolean endsWith = parts[columnIndex].trim().endsWith(suffix);
                                foundSuffix[0] = endsWith;
                                return endsWith;
                            }).map(parts -> parts[columnIndex].trim()).collect(Collectors.toList());

                    resultMap.put(columnName, result);
                }

                return resultMap;
            });
        } catch (IOException e) {
            handleIOException(e);
            return Collections.singletonMap("error", Collections.singletonList("An error occurred while processing the file."));
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

    private static <T> T handleException(IOExceptionHandler<T> handler) throws IOException {
        try {
            return handler.handle();
        } catch (IOException e) {
            handleIOException(e);
            throw e;
        }
    }

    private static void handleIOException(IOException e) {
        e.printStackTrace();
    }

    private interface IOExceptionHandler<T> {
        T handle() throws IOException;
    }


}

