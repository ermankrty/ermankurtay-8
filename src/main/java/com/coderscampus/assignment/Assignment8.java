package com.coderscampus.assignment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Assignment8 {

    private List<Integer> numbers = null;
    private AtomicInteger i = new AtomicInteger(0);

    public Assignment8() {
        try {
            numbers = Files.readAllLines(Paths.get("output.txt"))
                    .parallelStream() 
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Integer> getNumbersBatch() {
        int start, end;
        synchronized (i) {
            start = i.get();
            end = i.addAndGet(1000);
            System.out.println("Fetching records from " + start + " to " + end);
        }

        List<Integer> newList = IntStream.range(start, end)
                .mapToObj(numbers::get)
                .collect(Collectors.toList());

        System.out.println("Done Fetching records " + start + " to " + end);
        return newList;
    }

    public void processAsyncData() throws InterruptedException, ExecutionException {
        int totalTasks = 1000;
        int batchSize = 10; 
        List<CompletableFuture<List<Integer>>> futures = new ArrayList<>();

        for (int i = 0; i < totalTasks / batchSize; i++) {
            CompletableFuture<List<Integer>> future = CompletableFuture.supplyAsync(this::getNumbersBatch,
                    Executors.newFixedThreadPool(batchSize));
            futures.add(future);
        }

        CompletableFuture<Void> allTasks = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

        allTasks.get();

        List<Integer> allNumbers = futures.stream()
                .map(CompletableFuture::join)
                .flatMap(List::stream)
                .collect(Collectors.toList());

        Map<Integer, Long> numberOccurrences = allNumbers.stream()
                .collect(Collectors.groupingBy(i -> i, Collectors.counting()));

        numberOccurrences.forEach((number, count) -> System.out.println(number + "=" + count));
    }

    public static void main(String[] args) {
        try {
            new Assignment8().processAsyncData();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
