package com.example.batch_processor.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.example.batch_processor.model.Job;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Mono;

@Component
public class JobProducer { 
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic = "job-queue";
    public JobProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public Mono<Void> sendJob(Job job) { 
        return Mono.fromRunnable(() -> { 
            String jobMessage;
            try {
                jobMessage = new ObjectMapper().writeValueAsString(job);
                kafkaTemplate.send(topic, job.getId().toString(), jobMessage);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e.getMessage());
            }
            
        });
    }
}