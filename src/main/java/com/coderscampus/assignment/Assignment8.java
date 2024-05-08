package com.coderscampus.assignment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
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
                    .stream()
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<Integer> getNumbers() {
        int start, end;
        synchronized (i) {
            start = i.get();
            end = i.addAndGet(1000);
            System.out.println("Fetching records from \" + start + \" to \" + end");
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<Integer> newList = IntStream.range(start, end)
                .mapToObj(numbers::get)
                .collect(Collectors.toList());
        System.out.println("Done Fetching records " + start + " to " + end);
        return newList;
    }

    public List<Integer> getData() {
        ExecutorService fixedPool = Executors.newCachedThreadPool();
        List<CompletableFuture<List<Integer>>> futures = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            CompletableFuture<List<Integer>> future = CompletableFuture.supplyAsync(this::getNumbers, fixedPool);
            futures.add(future);
        }

        fixedPool.shutdown();

        return futures.stream()
                .map(CompletableFuture::join)
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    public void countAndPrintUniqueNumbers(List<Integer> allNumbers) {
        Map<Integer, Long> occurrences = allNumbers.parallelStream()
                .collect(Collectors.groupingByConcurrent(number -> number, Collectors.counting()));

        occurrences.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> System.out.println(entry.getKey() + "=" + entry.getValue()));
    }

    public static void main(String[] args) {
        Assignment8 assignment = new Assignment8();
        List<Integer> allNumbers = assignment.getData();
        assignment.countAndPrintUniqueNumbers(allNumbers);
    }
}
