package com.example.ticketmaster.exception;

public class EventNotFoundException extends RuntimeException {
    public EventNotFoundException(Long eventId) {
        super("Event with id " + eventId + " is not found");
    }
}