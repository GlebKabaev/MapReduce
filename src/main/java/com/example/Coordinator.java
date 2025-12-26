package com.example;

import com.example.task.MapTask;
import com.example.task.ReduceTask;
import com.example.task.Task;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Coordinator {

    private final Queue<MapTask> mapTasks = new LinkedList<>();
    private final Queue<ReduceTask> reduceTasks = new LinkedList<>();

    private int runningMapTasks = 0;
    private int runningReduceTasks = 0;
    private boolean isMapPhase = true;
    private boolean isFinished = false;

    private final int numReduces;
    private final int totalMapTasks;


    private final Lock lock = new ReentrantLock();

    private final Condition tasksChanged = lock.newCondition();

    public Coordinator(List<String> files, int numReduces) {
        this.numReduces = numReduces;
        this.totalMapTasks = files.size();

        for (int i = 0; i < files.size(); i++) {
            mapTasks.add(new MapTask(i, files.get(i), numReduces));
        }
    }

    public Task getTask() {
        lock.lock();
        try {
            while (true) {
                if (isFinished) {
                    return null;
                }

                if (isMapPhase) {

                    if (!mapTasks.isEmpty()) {
                        runningMapTasks++;
                        return mapTasks.poll();
                    } else if (runningMapTasks > 0) {
                        try {
                            tasksChanged.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return null;
                        }
                    } else {
                        isMapPhase = false;
                        initReduceTasks();
                        tasksChanged.signalAll();
                    }
                } else {

                    if (!reduceTasks.isEmpty()) {
                        runningReduceTasks++;
                        return reduceTasks.poll();
                    } else if (runningReduceTasks > 0) {
                        try {
                            tasksChanged.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return null;
                        }
                    } else {
                        isFinished = true;
                        tasksChanged.signalAll();
                        return null;
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void completeTask(Task task) {
        lock.lock();
        try {
            if (task instanceof MapTask) {
                runningMapTasks--;
            } else if (task instanceof ReduceTask) {
                runningReduceTasks--;
            }
            tasksChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void initReduceTasks() {
        System.out.println("Map phase end. Generating reduce tasks ");
        for (int i = 0; i < numReduces; i++) {
            reduceTasks.add(new ReduceTask(i, totalMapTasks));
        }
    }
}