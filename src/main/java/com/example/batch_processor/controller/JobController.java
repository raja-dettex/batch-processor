package com.example.batch_processor.controller;

import com.example.batch_processor.dtos.JobDto;
import com.example.batch_processor.model.Job;
import com.example.batch_processor.service.JobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.quartz.JobStoreType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/jobs")
public class JobController {

    @Autowired
    private JobService jobService;


    @PostMapping
    public Mono<Job> submitJob(@RequestBody JobDto jobDto) {
        return jobService.submitJob(jobDto);
    }

    @GetMapping("/{status}")
    public Flux<Job> getByStatus(@PathVariable String status) {
        return jobService.getJobsByStatus(status);
    }
}
