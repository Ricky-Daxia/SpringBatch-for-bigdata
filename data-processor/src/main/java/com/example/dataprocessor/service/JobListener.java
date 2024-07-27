package com.example.dataprocessor.service;

import lombok.NonNull;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Service;

@Service
public class JobListener implements JobExecutionListener {
//    private Logger logger = LoggerFactory.getLogger(this.getClass());

    public void beforeJob(@NonNull JobExecution jobExecution) {
//        logger.info("JOB IS STARTED.");
        System.out.println("before job");
    }

    public void afterJob(@NonNull JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.FAILED) {
//            logger.info("JOB IS EXECUTED FAILED.");
            System.out.println("failed");
        }
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
//            logger.info("JOB IS EXECUTED SUCCESSFULLY.");
            System.out.println("successful");
        }
    }
}