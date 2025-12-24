package com.example;


import com.example.task.MapTask;
import com.example.task.ReduceTask;

import java.io.IOException;


public class Main {
    public static void main(String[] args) {

        MapTask mapTask= new MapTask(12,"data.txt",1);

        ReduceTask reduceTask = new ReduceTask(12,1);
        Worker worker = new Worker();
        try {
            worker.work(mapTask);
            worker.work(reduceTask);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}