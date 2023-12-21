package org.innoboon.model.response;

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
        return String.format("(%s=%.2f)",columnName, sum);
    }
}
