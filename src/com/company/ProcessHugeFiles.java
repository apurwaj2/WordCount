package com.company;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class ProcessHugeFiles {

    /* Defining thread count in the variable THREAD_COUNT */
    private static final int THREAD_COUNT = 16;

    public static void main(String[] args) throws IOException, InterruptedException {
        long startTime = System.nanoTime(); // noting down start time of thread
        String file_name = "/home/apurwa/Downloads/8G";
        int K = 5; // K denotes the value of number of top frequency words to be printed

        /* Function called for reading and processing a file */
        List list = topKwords(file_name, K);
        System.out.println(list); // Displaying the list of the final output

        /* Displaying the time taken for execution */
        long endTime = System.nanoTime();
        long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
        System.out.println("Total elapsed time: " + elapsedTimeInMillis + " ms"); // Displaying the time taken by the process

    }

    public static List topKwords(String file_name, int K) throws IOException, InterruptedException {

        /* ConcurrentHashMap to store the words and their frequencies and
         LinkedBlockingDeque bounded to put in the readlines*/
        Map<String, Integer> map = new ConcurrentHashMap<String, Integer>();
        BlockingQueue<String> queue = new LinkedBlockingDeque<>(THREAD_COUNT * 2);

        /*New thread started for reading the file*/
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
                System.out.println("Value of i "+ i);
            }
        }.start();

        // Wait for the thread to start writing into the queue
        Thread.sleep(10);

        /*Executor service started with the specified thread count and invoking the threads with task instance*/
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

        /*Shutting down all the threads*/
        es.shutdown();
        es.awaitTermination(1, TimeUnit.SECONDS);

        List<String> ans = new ArrayList<String>();

        /*Using priority queue to find the top K by frequency words*/
        PriorityQueue<String> heap;
        heap = new PriorityQueue<String>(
                (w1, w2) -> map.get(w1).equals(map.get(w2)) ?
                        w2.compareTo(w1) : map.get(w1) - map.get(w2));

        for (String word : map.keySet()) {
            heap.offer(word);
            if (heap.size() > K) heap.poll();
        }

        while (!heap.isEmpty()) ans.add(heap.poll());
        Collections.reverse(ans);

        return ans;
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
                    line = queue.poll(1000, TimeUnit.MILLISECONDS); // polling queue to check for entries or end thread
                } catch (Exception e) {
                    e.printStackTrace();
                }

                String[] words = line.split("\\s+"); // split the read line into list of words

                for (String word : words) {
                    if (!word.isBlank()) {
                        word =  word.replaceAll("[^a-zA-Z0-9]+", ""); // replace special characters with nothing in each word
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
