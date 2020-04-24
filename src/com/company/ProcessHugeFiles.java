package com.company;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class ProcessHugeFiles {

    private static final int THREAD_COUNT = 12;

    public static void main(String[] args) throws IOException, InterruptedException {
        long startTime = System.nanoTime();
        String file_name = "/home/heisenberg/Downloads/32G";
        int K = 25;

        /* Function called for reading and processing a file */
        List list = topKwords(file_name, K);
        System.out.println(list);

        /* Displaying the time taken for execution */
        long endTime = System.nanoTime();
        long elapsedTimeInMillis = TimeUnit.MILLISECONDS.convert((endTime - startTime), TimeUnit.NANOSECONDS);
        System.out.println("Total elapsed time: " + elapsedTimeInMillis + " ms");

    }

    public static List topKwords(String file_name, int K) throws IOException, InterruptedException {

        Map<String, Integer> map = new ConcurrentHashMap<String, Integer>();
        BlockingQueue<String> queue = new LinkedBlockingDeque<>(THREAD_COUNT * 2);

        new Thread() {
            @Override
            public void run() {
                int i = 0;
                Scanner sc = null;
                try {
                    /* Reading file initialization */
                    sc = new Scanner(new FileInputStream(file_name), "UTF-8");
                    while (sc.hasNextLine()) {
                        queue.put(sc.nextLine());
                        i++;
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
                if (sc.ioException() != null) {
                    try {
                        throw sc.ioException();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
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
            // Future<Integer> result = es.submit(taskInstance);
        }

        List<Future<Integer>> result = null;

        try {
            result = es.invokeAll(taskList);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        es.shutdown();
        es.awaitTermination(5, TimeUnit.SECONDS);

        List<String> ans = new ArrayList<String>();

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
                    line = queue.poll(1000, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                String[] words = line.split("\\s+");

                for (String word : words) {
                    if (!word.isBlank()) {
                        word =  word.replaceAll("[^a-zA-Z0-9]+", "");
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
