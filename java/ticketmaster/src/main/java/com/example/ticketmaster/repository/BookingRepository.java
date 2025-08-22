package com.example.ticketmaster.repository;

import com.example.ticketmaster.model.Booking;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface BookingRepository extends JpaRepository<Booking, Long> {
    List<Booking> findByUserId(Long userId);
    boolean existsByUserIdAndEventId(Long userId, Long eventId);
}
