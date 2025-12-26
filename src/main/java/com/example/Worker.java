package com.example;

import com.example.task.MapTask;
import com.example.task.ReduceTask;
import com.example.task.Task;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Worker {
    private List<KeyValue> map(String filename, String content) {
        List<KeyValue> keysValues = new ArrayList<>();
        List<String> words = List.of(content.split(" "));
        words.forEach(word -> keysValues.add(new KeyValue(word, "1")));
        return keysValues;
    }

    private String reduce(String key, List<String> values) {
        return String.valueOf(values.size());
    }

    public void work(Task task) throws IOException {
        switch (task) {
            case MapTask mapTask -> {
                System.out.println(Thread.currentThread().getName() + " doing MapTask " + mapTask.getId());
                String content = readFileAsString(mapTask.getFilename());
                writeIntermediateFiles(map(mapTask.getFilename(), content), mapTask);
            }
            case ReduceTask reduceTask -> {
                System.out.println(Thread.currentThread().getName() + " doing ReduceTask " + reduceTask.getId());
                processReduce(reduceTask);
            }

            default -> throw new IllegalStateException("Unexpected value: " + task);
        }
    }

    private String readFileAsString(String filename) throws IOException {
        return Files.readString(Paths.get(filename));
    }

    private void writeIntermediateFiles(List<KeyValue> keyValueList, MapTask task) {
        Map<Integer, BufferedWriter> writers = new HashMap<>();
        int numReduces = task.getNumReduces();

        try {
            for (KeyValue kv : keyValueList) {
                int reduceId = Math.abs(kv.getKey().hashCode()) % numReduces;

                if (!writers.containsKey(reduceId)) {
                    String fileName = "mr-temp-" + task.getId() + "-" + reduceId;
                    writers.put(reduceId, new BufferedWriter(new FileWriter(fileName, true)));
                }

                BufferedWriter writer = writers.get(reduceId);
                writer.write(kv.getKey() + " " + kv.getValue());
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error writing intermediate file", e);
        } finally {
            for (BufferedWriter writer : writers.values()) {
                try {
                    writer.close();
                } catch (IOException e) {

                }
            }
        }
    }

    private void processReduce(ReduceTask reduceTask) {
        int reduceId = reduceTask.getId();
        int numMaps = reduceTask.getNumMaps();
        Map<String, List<String>> keyToValues = new HashMap<>();

        for (int mapId = 0; mapId < numMaps; mapId++) {
            String inputFile = "mr-temp-" + mapId + "-" + reduceId;

            File f = new File(inputFile);
            if (!f.exists()) continue;
            try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(" ", 2);
                    if (parts.length == 2) {
                        keyToValues.computeIfAbsent(parts[0], k -> new ArrayList<>()).add(parts[1]);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading intermediate file: " + inputFile, e);
            }
        }

        String outputFileName = "mr-out-" + reduceId;
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName))) {
            List<String> keys = new ArrayList<>(keyToValues.keySet());
            Collections.sort(keys);

            for (String key : keys) {
                String result = reduce(key, keyToValues.get(key));
                writer.write(key + " " + result);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error processing reduce task", e);
        }
    }
}