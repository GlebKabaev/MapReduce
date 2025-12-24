package com.example.task;


import lombok.Getter;

import java.util.List;

@Getter
public class ReduceTask extends Task {

    private final Integer numMaps;

    public ReduceTask(int id, Integer numMaps) {
        this.numMaps = numMaps;
        this.id = id;

    }

}
