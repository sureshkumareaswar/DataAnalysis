package org.example.model;

import lombok.*;

@Data
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class ResponseData {
    private String columnName;
    private double sum;

    @Override
    public String toString() {
        return String.format("(sum=%.2f)", sum);
    }
}
