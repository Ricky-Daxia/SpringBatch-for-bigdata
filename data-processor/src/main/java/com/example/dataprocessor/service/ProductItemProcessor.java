package com.example.dataprocessor.service;

import com.example.dataprocessor.model.Product;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;

public class ProductItemProcessor implements ItemProcessor<JsonNode, Product>, StepExecutionListener {

    private ObjectMapper objectMapper;

    @Override
    public void beforeStep(@NonNull StepExecution stepExecution) {
        System.out.println("before ProductItemProcessor");
        objectMapper = new ObjectMapper();
    }

    @Override
    public ExitStatus afterStep(@NonNull StepExecution stepExecution) {
        System.out.println("after ProductItemProcessor");
        return null;
    }

    @Override
    public Product process(@NonNull JsonNode jsonNode) throws Exception {
        return objectMapper.treeToValue(jsonNode, Product.class);
    }
}
