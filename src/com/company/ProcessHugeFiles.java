package com.company;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class ProcessHugeFiles {

    private static final int THREAD_COUNT = 4;
    private static final String EOF = "EOF";
    
    public static void main(String[] args) throws IOException, InterruptedException {
        long startTime = System.nanoTime();
        String file_name = "/home/heisenberg/Downloads/400M";
        int K = 25;

        /* Function called for reading and processing a file */
        topKwords(file_name, K);

        /* Displaying the time taken for execution */
        long endTime = System.nanoTime();
        long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
        System.out.println("Total elapsed time: " + elapsedTimeInMillis + " ms");

    }

    public static void topKwords(String file_name, int K) throws IOException, InterruptedException {

        Map<String, Integer> map = new ConcurrentHashMap<String, Integer>();
        BlockingQueue<String> queue = new LinkedBlockingDeque<>();

        new Thread() {
            @Override
            public void run() {
                int i = 0;
                try {
                    /* Reading file initialization */
                    String line = null;
                    BufferedReader br = new BufferedReader(new FileReader(file_name));
                    while ((line = br.readLine()) != null) {
                        queue.put(line);
                        i++;
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Value of i " + i);
            }
        }.start();

        // Wait for the thread to start writing into the queue
        Thread.sleep(100);

        ExecutorService es = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Task> taskList = new ArrayList<>();
        for (int i = 0; i < THREAD_COUNT; i++) {
            Task taskInstance = new Task(map, queue);
            taskList.add(taskInstance);
        }

        List<Future<Integer>> result = null;

        try {
            result = es.invokeAll(taskList);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        es.shutdown();
        //  es.awaitTermination(5, TimeUnit.SECONDS);

        int m = 0;
        List<String> res = new ArrayList<String>();

        Object[] a = map.entrySet().toArray();
        Arrays.sort(a, new Comparator() {
            public int compare(Object o1, Object o2) {
                return ((Map.Entry<String, Integer>) o2).getValue()
                        .compareTo(((Map.Entry<String, Integer>) o1).getValue());
            }
        });
        for (Object e : a) {
            res.add(((Map.Entry<String, Integer>) e).getKey());
            if(m++ > K)
                break;
        }

        System.out.println(res);
    }

    private static class Task implements Callable<Integer> {

        private Map<String, Integer> map;
        private BlockingQueue<String> queue;

        public Task(Map<String, Integer> map, BlockingQueue<String> queue) {
            this.map = map;
            this.queue = queue;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                String line = null;
                try {
                    line = queue.poll(1000, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String[] words = line.split("\\s+");

                for (String word : words) {
                    if (!word.isBlank()) {
                        word.replaceAll("[^a-zA-Z0-9]+", "");
                        if (!map.containsKey(word)) {
                            map.putIfAbsent(word, 1);
                        } else {
                            map.put(word, map.get(word) + 1);
                        }
                    }
                }
            }
        }
    }
}