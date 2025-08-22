package com.example;

import java.util.List;
import java.util.stream.Collectors;

public class ArticleFilter {
    public static List<Article> filterByKeyword(List<Article> articles, String keyword) {
        String lowerKeyword = keyword.toLowerCase();
        return articles.stream()
            .filter(article -> article.getTitle().toLowerCase().contains(lowerKeyword) ||
                article.getContent().toLowerCase().contains(lowerKeyword))
                .collect((Collectors.toList()));
    }
}
