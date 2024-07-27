package com.example.dataprocessor.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class JsonFileReader implements ItemReader<JsonNode>, StepExecutionListener {

    private BufferedReader reader;

    private ObjectMapper objectMapper;

    private final String fileName;

    public JsonFileReader(String file) {
        if (file.matches("^file:(.*)")) {
            file = file.substring(file.indexOf(":") + 1);
        }
        fileName = file;
    }

    private void initReader() throws FileNotFoundException {
        File file = new File(fileName);
        reader = new BufferedReader(new FileReader(file), 65535);
    }

    @Override
    public void beforeStep(@NonNull StepExecution stepExecution) {
        System.out.println("before file reader");
    }

    @Override
    public ExitStatus afterStep(@NonNull StepExecution stepExecution) {
        System.out.println("after file reader");
        return null;
    }

    @Override
    public synchronized JsonNode read() throws Exception {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        if (reader == null) {
            initReader();
        }
        String line = reader.readLine();
        if (line != null) {
//            return objectMapper.readTree(reader.readLine());
//            System.out.println(line);
            return objectMapper.readTree(line);
        }
        return null;
    }

}
