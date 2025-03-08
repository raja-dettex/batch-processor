package com.example.batch_processor.service;


import com.example.batch_processor.dtos.JobDto;
import com.example.batch_processor.model.Job;
import com.example.batch_processor.producer.JobProducer;
import com.example.batch_processor.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

@Service
public class JobService {
    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobProducer jobProducer;

    public Mono<Job> submitJob(JobDto jobdto) {
        Job job = new Job(null, jobdto.getType(), "PENDING", jobdto.getPriority());
        return jobRepository.save(job)
            .flatMap(savedJob -> jobProducer.sendJob(savedJob)).thenReturn(job);
    }

    public Flux<Job> getJobsByStatus(String status) {
        return jobRepository.findByStatusOrderByPriorityDesc(status).delayElements(Duration.ofSeconds(1));
    }

    public Mono<Job> processJob(Job job) { 
        return jobRepository.findById(Mono.just(job.getId()))
            .map(existingJob -> {
                existingJob.setStatus("IN_PROGRESS");
                jobRepository.save(existingJob).subscribe();
                return existingJob;
                
            })
            .doOnNext(updatedJob -> {System.out.println("updated job: " + updatedJob.getId());})
            .map(existingJob -> {
                existingJob.setStatus("COMPLETED");
                jobRepository.save(existingJob).subscribe();
                return existingJob;
            })
            .onErrorResume(e -> { 
                job.setStatus("FAILED");
                jobRepository.save(job);
                return Mono.just(job);
            });
    }
}
