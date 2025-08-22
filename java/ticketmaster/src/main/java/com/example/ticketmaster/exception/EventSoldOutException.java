package com.example.ticketmaster.exception;

public class EventSoldOutException extends RuntimeException {
    public EventSoldOutException(Long eventId) {
        super("Event with id " + eventId + " is sold out");
    }
}