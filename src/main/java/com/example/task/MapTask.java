package com.example.task;

import lombok.Getter;


@Getter
public class MapTask extends Task {

    private final String filename;
    private final Integer numReduces;

    public MapTask(int id, String filename, int numReduces) {
        this.id = id;
        this.filename = filename;
        this.numReduces = numReduces;
    }

}