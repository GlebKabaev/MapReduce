package com.example;

import com.example.task.ReduceTask;
import com.example.task.Task;
import com.example.task.MapTask;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


public class Worker {
    public List<KeyValue> map(String filename, String content) {
        List<KeyValue> keysValues = new ArrayList<>();
        List<String> words = List.of(content.split(" "));
        words.forEach(word -> keysValues.add(new KeyValue(word, "1")));
        return keysValues;
    }

    public String reduce(String key, List<String> values) {
        return String.valueOf(values.size());
    }

    public void work(Task task) throws IOException {
        switch (task) {
            case MapTask mapTask -> {

                String content = readFileAsString(mapTask.getFilename());
                writeFiles(map(mapTask.getFilename(), content), mapTask);
            }

            case ReduceTask reduceTask -> {
                processReduce(reduceTask);
            }

            default -> throw new IllegalStateException("Unexpected value: " + task);
        }
    }


    private String readFileAsString(String filename) throws IOException {
        return Files.readString(Paths.get(filename));
    }

    private void writeFiles(List<KeyValue> keyValueList, MapTask task) {
        Map<Integer, BufferedWriter> writers = new HashMap<>();
        Integer numReduces = task.getNumReduces();
        try {

            for (int i = 0; i < numReduces; i++) {
                String fileName = "mr-temp-" + task.getId() + '-' + i;
                BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
                writers.put(i, writer);
            }


            for (KeyValue kv : keyValueList) {
                int reduceId = Math.abs(kv.getKey().hashCode()) % numReduces;
                BufferedWriter writer = writers.get(reduceId);
                writer.write(kv.getKey() + " " + kv.getValue());
                writer.newLine();
            }

        } catch (IOException e) {
            throw new RuntimeException("Ошибка записи промежуточных файлов", e);
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
        String outputFileName = "mr-out-" + reduceId;
        BufferedWriter outputWriter = null;

        try {

            for (int m = 0; m < numMaps; m++) {
                String inputFile = "mr-temp-" + reduceId + "-" + m;
                try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split(" ", 2);
                        if (parts.length == 2) {
                            String key = parts[0];
                            String value = parts[1];
                            keyToValues.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Ошибка чтения промежуточного файла: " + inputFile, e);
                }
            }


            outputWriter = new BufferedWriter(new FileWriter(outputFileName));


            List<String> keys = new ArrayList<>(keyToValues.keySet());
            Collections.sort(keys);

            for (String key : keys) {
                String result = reduce(key, keyToValues.get(key));
                outputWriter.write(key + " " + result);
                outputWriter.newLine();
            }

        } catch (IOException e) {
            throw new RuntimeException("Ошибка обработки reduce задачи", e);
        } finally {
            if (outputWriter != null) {
                try {
                    outputWriter.close();
                } catch (IOException e) {

                }
            }
        }
    }
}

