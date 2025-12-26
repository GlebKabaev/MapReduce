package com.example;

import com.example.task.Task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) throws InterruptedException {

        List<String> inputFiles = Arrays.asList("file1.txt", "file2.txt", "file3.txt");


        int workers = 3;
        int reduces = 3;

        Coordinator coordinator = new Coordinator(inputFiles, reduces);


        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < workers; i++) {
            Thread thread = new Thread(() -> {
                Worker worker = new Worker();
                while (true) {
                    Task task = coordinator.getTask();

                    if (task == null) {
                        break;
                    }

                    try {
                        worker.work(task);
                        coordinator.completeTask(task);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });

            thread.setName("Worker-" + i);
            thread.start();
            threads.add(thread);
        }


        for (Thread t : threads) {
            t.join();
        }

        System.out.println("MapReduce end");
    }
}