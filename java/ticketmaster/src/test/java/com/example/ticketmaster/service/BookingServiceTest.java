package com.example.ticketmaster.service;

import com.example.ticketmaster.model.Booking;
import com.example.ticketmaster.model.Event;
import com.example.ticketmaster.model.User;
import com.example.ticketmaster.repository.BookingRepository;
import com.example.ticketmaster.repository.EventRepository;
import com.example.ticketmaster.repository.UserRepository;
import com.example.ticketmaster.service.BookingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional // rolls back after each test
class BookingServiceTest {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private BookingRepository bookingRepository;

    @Autowired
    private BookingService bookingService;

    private User testUser1;
    private User testUser2;
    private Event testEvent;

    @BeforeEach
    void setUp() {
        // Create dummy users
        testUser1 = userRepository.save(new User(null, "Alice", "testalice@example.com", "hashedPassword1"));
        testUser2 = userRepository.save(new User(null, "Bob", "testbob@example.com", "hashedPassword2"));

        // Create dummy event
        testEvent = eventRepository.save(new Event(null, "Concert", "Victoria Concert Hall", 10, 10, LocalDateTime.now().plusDays(1), 0L));
    }

    @Test
    void testSingleBooking() {
        Booking booking = bookingService.bookTicket(testUser1.getId(), testEvent.getId());

        assertNotNull(booking.getId());
        assertEquals(testUser1.getId(), booking.getUserId());
        assertEquals(testEvent.getId(), booking.getEventId());
        assertNotNull(booking.getCreatedAt());
    }

    @Test
    void testMultipleBookings() {
        Booking booking1 = bookingService.bookTicket(testUser1.getId(), testEvent.getId());
        Booking booking2 = bookingService.bookTicket(testUser2.getId(), testEvent.getId());

        List<Booking> bookings = bookingRepository.findAll();
        assertEquals(4, bookings.size());

        assertTrue(bookings.stream().anyMatch(b -> b.getUserId().equals(testUser1.getId())));
        assertTrue(bookings.stream().anyMatch(b -> b.getUserId().equals(testUser2.getId())));
    }
}
