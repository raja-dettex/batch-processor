package com.example.batch_processor.producer;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.batch_processor.model.Job;
import com.example.batch_processor.repository.JobRepository;
import com.example.batch_processor.service.JobService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;


@Component
@Slf4j
public class JobConsumer {


    @Autowired
    private JobService jobService;
    
    @KafkaListener(topics = "job-queue", groupId = "job-processing-group", containerFactory = "KafkaListenerContainerFactory")
    public void consumeBatch(List<ConsumerRecord<String, String>>jobs)  {
        System.out.println("here");
        System.out.println(jobs.isEmpty());
        System.out.println(jobs.toString());
        
        Flux.fromIterable(jobs)
            .map(ConsumerRecord::value)
            .map(this::mapToJob)
            .bufferTimeout(5, Duration.ofSeconds(2))
            .flatMap(this::processJobs)
            .doOnNext(job -> { System.out.println(job);})
            .subscribe();
    }
    private Job mapToJob(String message)  { 
        System.out.println(message);
        try {
            return new ObjectMapper().readValue(message, Job.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("error deserializing jobs : " + e.getMessage());
        }
    }

    private Flux<Job> processJobs(List<Job> jobs) { 
        System.out.println("processing jobs");
        return Flux.fromIterable(jobs)
            .flatMap(job -> { 
                return this.jobService.processJob(job);
            });
    }
}
