package org.example.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FileUtility {
    /**
     * Reads a CSV file, finds the specified column, and calculates the sum of its numeric values.
     *
     * @param filePath   The path to the CSV file.
     * @param columnName The name of the column for which to calculate the sum.
     * @return The sum of the numeric values in the specified column.
     * @throws RuntimeException If an error occurs during file reading or parsing.
     */
    public static double sum(String filePath, String columnName) {
        try {
            return handleException(() -> {
                try (CSVParser parser = CSVParser.parse(new FileReader(filePath), CSVFormat.DEFAULT.withHeader())) {
                    int columnIndex = findColumnIndex(parser, columnName);
                    if (columnIndex == -1) {
                        throw new IllegalArgumentException("Column not found: " + columnName);
                    }
                    double sum = 0.0;
                    // Iterate through each record in the CSV file
                    for (CSVRecord record : parser) {
                        String value = record.get(columnIndex);
                        // Check if the value is not empty before adding to the sum
                        if (!value.isEmpty()) {
                            sum += Double.parseDouble(value);
                        }
                    }
                    return sum;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private static int findColumnIndex(CSVParser parser, String columnName) {
        for (int i = 0; i < parser.getHeaderMap().size(); i++) {
            String headerName = parser.getHeaderMap().keySet().toArray()[i].toString();
            if (headerName.equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Calculates the average of numeric values in a specified column of a CSV file.
     *
     * @param filePath   The path to the CSV file.
     * @param columnName The name of the column for which the average is calculated.
     * @return The average of numeric values in the specified column.
     * @throws RuntimeException If an error occurs during file reading or parsing.
     */
    public static double average(String filePath, String columnName) {
        try {
            return handleException(() -> {
                List<String> lines = Files.readAllLines(Paths.get(filePath));
                int columnIndex = getHeaderIndex(lines.get(0).split(","), columnName);
                // Check if the column exists in the header
                if (columnIndex == -1) {
                    throw new IllegalArgumentException("Column '" + columnName + "' not found in the CSV file.");
                }

                return lines.stream().skip(1).map(line -> line.split(",")).filter(parts -> parts.length > columnIndex)
                        .mapToDouble(parts -> Double.parseDouble(parts[columnIndex]
                                .trim())).average().orElseThrow(() -> new IllegalArgumentException("No valid data in the specified column."));
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieves a list of values from a specified column in a CSV file that start with a given prefix.
     *
     * @param filePath   The path to the CSV file.
     * @param columnName The name of the column to search for values.
     * @param prefix     The prefix to match at the beginning of the values.
     * @return A list of values from the specified column that start with the given prefix.
     */

    public static List<String> startsWith(String filePath, String columnName, String prefix) {
        try {
            return handleException(() -> {
                List<String> lines = Files.readAllLines(Paths.get(filePath));
                int columnIndex = getHeaderIndex(lines.get(0).split(","), columnName);
                if (columnIndex == -1) {
                    throw new IllegalArgumentException("Column '" + columnName + "' not found in the CSV file.");
                }

                final boolean[] foundPrefix = {false};
                return lines.stream().skip(1) // Skip the header row
                        .map(line -> line.split(",")).filter(parts -> parts.length > columnIndex).filter(parts -> {
                            // Use a flag to track whether the prefix has been found
                            if (foundPrefix[0]) {
                                return false;  // Skip values until the next occurrence after the prefix
                            }
                            boolean startsWith = parts[columnIndex].trim().startsWith(prefix);
                            foundPrefix[0] = startsWith;
                            return startsWith;
                        }).map(parts -> parts[columnIndex].trim()).collect(Collectors.toList());
            });
        } catch (IOException e) {
            handleIOException(e);
            return Collections.emptyList();
        }
    }

    /**
     * Retrieves a list of values from a specified column in a CSV file that end with a given suffix.
     *
     * @param filePath   The path to the CSV file.
     * @param columnName The name of the column to search for values.
     * @param suffix     The suffix to match at the end of the values.
     * @return A list of values from the specified column that end with the given suffix.
     */
    public static List<String> endsWith(String filePath, String columnName, String suffix) {
        try {
            return handleException(() -> {
                List<String> lines = Files.readAllLines(Paths.get(filePath));
                int columnIndex = getHeaderIndex(lines.get(0).split(","), columnName);
                if (columnIndex == -1) {
                    throw new IllegalArgumentException("Column '" + columnName + "' not found in the CSV file.");
                }

                final boolean[] foundSuffix = {false};
                return lines.stream().skip(1) // Skip the header row
                        .map(line -> line.split(",")).filter(parts -> parts.length > columnIndex).filter(parts -> {
                            if (foundSuffix[0]) {
                                return false;  // Skip values until the next occurrence after the suffix
                            }
                            boolean endsWith = parts[columnIndex].trim().endsWith(suffix);
                            foundSuffix[0] = endsWith;
                            return endsWith;
                        }).map(parts -> parts[columnIndex].trim()).collect(Collectors.toList());
            });
        } catch (IOException e) {
            handleIOException(e);
            return Collections.emptyList();
        }
    }

    private static int getHeaderIndex(String[] headers, String columnName) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equals(columnName)) {
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
        // You can log the exception or perform other error handling here
        e.printStackTrace();
    }

    private interface IOExceptionHandler<T> {
        T handle() throws IOException;
    }


}

