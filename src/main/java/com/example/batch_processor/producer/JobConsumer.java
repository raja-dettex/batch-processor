package com.example.batch_processor.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.example.batch_processor.model.Job;
import com.example.batch_processor.repository.JobRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;


@Component
@Slf4j
public class JobConsumer {

    @Autowired
    private JobRepository jobRepository;
    
    @KafkaListener(topics = "job-queue", groupId = "job-processing-group")
    public void consume(String message)  { 
        Job job;
        try {
            job = new ObjectMapper().readValue(message, Job.class);
            job.setStatus("COMPLETED");
            jobRepository.save(job).subscribe(savedJob -> { 
            log.info("job completed with job id : {}" , job.getId());
        });
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e.getMessage());
        } 
        
    }
}
