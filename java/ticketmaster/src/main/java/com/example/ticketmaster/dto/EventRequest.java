package com.example.ticketmaster.dto;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.time.LocalDateTime;

@Data
public class EventRequest {
    @NotNull
    private String name;

    @NotNull
    private LocalDateTime dateTime;

    @NotNull
    private String location;

    @NotNull
    private Integer capacity;
}
