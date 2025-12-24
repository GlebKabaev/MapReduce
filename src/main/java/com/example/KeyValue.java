package com.example;

import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class KeyValue {
    private String key;
    private String value;

    public KeyValue(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
