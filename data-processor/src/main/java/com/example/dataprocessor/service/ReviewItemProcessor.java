package com.example.dataprocessor.service;

import com.example.dataprocessor.model.Review;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;

public class ReviewItemProcessor implements ItemProcessor<JsonNode, Review>, StepExecutionListener {

    private ObjectMapper objectMapper;

    @Override
    public void beforeStep(@NonNull StepExecution stepExecution) {
        System.out.println("before ReviewItemProcessor");
        objectMapper = new ObjectMapper();
    }

    @Override
    public ExitStatus afterStep(@NonNull StepExecution stepExecution) {
        System.out.println("after ReviewItemProcessor");
        return null;
    }

    @Override
    public Review process(@NonNull JsonNode jsonNode) throws Exception {
        return objectMapper.treeToValue(jsonNode, Review.class);
    }
}
