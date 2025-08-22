package com.example.ticketmaster.dto;

import jakarta.validation.constraints.NotNull;

import lombok.Data;

@Data
public class BookingRequest {
    @NotNull(message = "eventId is required")
    private Long eventId;
}