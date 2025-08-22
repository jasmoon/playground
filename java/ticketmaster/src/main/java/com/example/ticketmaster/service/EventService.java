package com.example.ticketmaster.service;

import com.example.ticketmaster.dto.EventRequest;
import com.example.ticketmaster.model.Event;
import com.example.ticketmaster.repository.EventRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EventService {

    private final EventRepository eventRepository;

    public EventService(EventRepository eventRepository) {
        this.eventRepository = eventRepository;
    }

    public Event createEvent(EventRequest request) {
        Event event = new Event();
        event.setName(request.getName());
        event.setLocation(request.getLocation());
        event.setDateTime(request.getDateTime());
        event.setCapacity(request.getCapacity());
        event.setAvailableSeats(request.getCapacity());
        return eventRepository.save(event);
    }

    public List<Event> getAllEvents() {
        return eventRepository.findAll();
    }

    public Event getEventById(Long id) {
        return eventRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Event not found"));
    }

    public Event updateEvent(Long id, Event updatedEvent) {
        Event existing = getEventById(id);
        existing.setName(updatedEvent.getName());
        existing.setLocation(updatedEvent.getLocation());
        existing.setDateTime(updatedEvent.getDateTime());
        return eventRepository.save(existing);
    }

    public void deleteEvent(Long id) {
        eventRepository.deleteById(id);
    }

    public List<Event> searchEvents(String keyword) {
        return eventRepository.searchEvents(keyword);
    }
}
