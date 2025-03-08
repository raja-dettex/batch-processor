package com.example.batch_processor.repository;

import com.example.batch_processor.model.Job;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.util.UUID;

@Repository
public interface JobRepository extends ReactiveCrudRepository<Job, UUID> {
    Flux<Job> findByStatusOrderByPriorityDesc(String status);
}
