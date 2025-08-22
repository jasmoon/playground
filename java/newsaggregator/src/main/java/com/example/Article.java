package com.example;

import java.time.LocalDateTime;

public class Article {
    private String title;
    private String content;
    private String source;
    private LocalDateTime publishedAt;
    private String category;

    public Article(String title, String source, String content, LocalDateTime publishedAt, String category) {
        this.title = title;
        this.source = source;
        this.content = content;
        this.publishedAt = publishedAt;
        this.category = category;
    }

    public String getTitle() { return title; }
    public String getContent() { return content; }
    public String getSource() { return source; }
    public LocalDateTime getPublishedAt() { return publishedAt; }
    public String getCategory() { return category; }

    @Override
    public String toString() {
        return String.format("[%s] %s (%s)", category, title, source);
    }
}
