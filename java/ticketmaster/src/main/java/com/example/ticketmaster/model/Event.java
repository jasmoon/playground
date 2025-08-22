// src/main/java/com/example/ticketmaster/model/Event.java
package com.example.ticketmaster.model;

import java.time.LocalDateTime;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "events")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;
    private String location;
    private int capacity;

    @Column(name = "available_seats")
    private int availableSeats;

    @Column(name = "date_time", nullable = false)
    private LocalDateTime dateTime;

    @Version
    private Long version; // Optimistic locking version
}
