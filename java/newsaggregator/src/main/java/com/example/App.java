package com.example;

import java.util.List;

public class App {
    public static void main(String[] args) {
        NewsSource source1 = new DummyNewsSource("Source A");
        NewsSource source2 = new DummyNewsSource("Source B");

        List<Article> allArticles = source1.fetchArticles();
        allArticles.addAll(source2.fetchArticles());

        System.out.println("=== Aggregated News ===");
        allArticles.forEach(article -> System.out.println(article));
    }
}
