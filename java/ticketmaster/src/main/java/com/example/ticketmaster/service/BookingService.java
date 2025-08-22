package com.example.ticketmaster.service;

import com.example.ticketmaster.exception.EventNotFoundException;
import com.example.ticketmaster.exception.EventSoldOutException;
import com.example.ticketmaster.model.*;
import com.example.ticketmaster.repository.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
public class BookingService {
    private final BookingRepository bookingRepo;
    private final EventRepository eventRepo;

    public BookingService(BookingRepository bookingRepo, EventRepository eventRepo) {
        this.bookingRepo = bookingRepo;
        this.eventRepo = eventRepo;

    }

    @Transactional(isolation = Isolation.SERIALIZABLE)
    public Booking bookTicket(Long userId, Long eventId) {
        // Check if event exists
        Event event = eventRepo.findById(eventId)
            .orElseThrow(() -> new EventNotFoundException(eventId));

        if (bookingRepo.existsByUserIdAndEventId(userId, eventId)) {
            throw new RuntimeException("User already booked this event");
        }

        if (event.getAvailableSeats() <= 0) {
            throw new EventSoldOutException(eventId);
        }

        event.setAvailableSeats(event.getAvailableSeats() - 1);
        eventRepo.save(event);

        Booking booking = new Booking(null, userId, eventId, LocalDateTime.now());
        return bookingRepo.save(booking);
    }
}
