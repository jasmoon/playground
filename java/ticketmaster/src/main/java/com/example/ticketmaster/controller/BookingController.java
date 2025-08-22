package com.example.ticketmaster.controller;

import com.example.ticketmaster.dto.BookingRequest;
import com.example.ticketmaster.model.Booking;
import com.example.ticketmaster.model.User;
import com.example.ticketmaster.service.BookingService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/bookings")
@RequiredArgsConstructor
public class BookingController {
    private static final Logger log = LoggerFactory.getLogger(BookingController.class);

    private final BookingService bookingService;

    @PostMapping
    public Booking book(@Valid @RequestBody BookingRequest request, Authentication authentication) {
        log.info("Received booking request: eventId={}, user={}", request.getEventId(), authentication.getName());

        User user = (User) authentication.getPrincipal();
        try {
            Booking booking = bookingService.bookTicket(user.getId(), request.getEventId());
            log.info("Booking successful: bookingId={} for user={}", booking.getId(), user.getId());
            return booking;
        } catch (ObjectOptimisticLockingFailureException ex) {
            log.warn("Seat conflict for eventId={} user={}. Retrying required.", request.getEventId(), user.getId());
            throw new RuntimeException("Seat was taken, please try again");
        } catch (Exception e) {
            log.error("Unexpected error booking eventId={} user={}", request.getEventId(), user.getId(), e);
            throw e;
        }
    }
}
