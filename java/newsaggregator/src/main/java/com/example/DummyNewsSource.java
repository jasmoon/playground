package com.example;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class DummyNewsSource implements NewsSource {
    private String sourceName;

    public DummyNewsSource(String sourceName) {
        this.sourceName = sourceName;
    }

    @Override
    public List<Article> fetchArticles() {
        List<Article> articles = new ArrayList<>();
        articles.add(new Article("Breaking News 1", sourceName, LocalDateTime.now(), "Politics"));
        articles.add(new Article("Tech Update", sourceName, LocalDateTime.now().minusHours(1), "Technology"));
        return articles;
    }
}
