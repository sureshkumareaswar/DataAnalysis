package org.example.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class FileUtility {
    public static double sum(String filePath, String columnName) throws IOException {
        try (CSVParser parser = CSVParser.parse(new FileReader(filePath), CSVFormat.DEFAULT.withHeader())) {
            int columnIndex = findColumnIndex(parser, columnName);
            if (columnIndex == -1) {
                throw new IllegalArgumentException("Column not found: " + columnName);
            }
            double sum = 0.0;
            for (CSVRecord record : parser) {
                String value = record.get(columnIndex);
                if (!value.isEmpty()) {
                    sum += Double.parseDouble(value);
                }
            }
            return sum;
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

    public static double average(String filePath, String columnName) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filePath));

        int columnIndex = getHeaderIndex(lines.get(0).split(","), columnName);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column '" + columnName + "' not found in the CSV file.");
        }

        return lines.stream().skip(1).map(line -> line.split(",")).filter(parts -> parts.length > columnIndex).mapToDouble(parts -> Double.parseDouble(parts[columnIndex].trim())).average().orElse(0.0);
    }

    public static List<String> startsWith(String filePath, String columnName, String prefix) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filePath));

        int columnIndex = getHeaderIndex(lines.get(0).split(","), columnName);
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column '" + columnName + "' not found in the CSV file.");
        }

        final boolean[] foundPrefix = {false};
        return lines.stream().skip(1) // Skip the header row
                .map(line -> line.split(",")).filter(parts -> parts.length > columnIndex).filter(parts -> {
                    if (foundPrefix[0]) {
                        return false;  // Skip values until the next occurrence after the prefix
                    }
                    boolean startsWith = parts[columnIndex].trim().startsWith(prefix);
                    foundPrefix[0] = startsWith;
                    return startsWith;
                }).map(parts -> parts[columnIndex].trim()).collect(Collectors.toList());
    }

    public static List<String> endsWith(String filePath, String columnName, String suffix) throws IOException {
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
    }

    private static int getHeaderIndex(String[] headers, String columnName) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equals(columnName)) {
                return i;
            }
        }
        return -1;
    }

}

