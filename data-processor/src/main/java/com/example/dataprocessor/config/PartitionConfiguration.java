package com.example.dataprocessor.config;

import com.example.dataprocessor.model.Product;
import com.example.dataprocessor.model.Review;
import com.example.dataprocessor.service.*;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.net.MalformedURLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

//@Configuration
//@Lazy
public class PartitionConfiguration {

    @Bean
    public Job productJob(JobRepository jobRepository, Step masterStep) {
        return new JobBuilder("productJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(new JobListener())
                .flow(masterStep)
                .end().build();
    }

    @Bean
    public Step masterStep(JobRepository jobRepository,
                           Step slaveStep,
                           Partitioner partitioner,
                           TaskExecutor subTaskExecutor) {
        return new StepBuilder("master step", jobRepository)
                .partitioner(slaveStep)
                .partitioner("partitioner", partitioner)
                .gridSize(5)
                .taskExecutor(subTaskExecutor)
                .build();
    }

    @Bean
    public Partitioner partitioner() throws Exception {
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        partitioner.setResources(resolver.getResources("Digital_Music_*"));
        return partitioner;
    }

    @Bean
    public Step slaveStep(JobRepository jobRepository,
                          DataSourceTransactionManager dataSourceTransactionManager,
                          DataSource dataSource,
                          SynchronizedItemStreamReader<Review> partitionReader,
                          TaskExecutor mainTaskExecutor) {
        return new StepBuilder("slave step", jobRepository)
                .<Review, Review>chunk(500, dataSourceTransactionManager)
                .reader(partitionReader)
                .processor(item -> item)
                .writer(new ReviewWriter(dataSource))
                .taskExecutor(mainTaskExecutor)
                .build();
    }

    @Bean
    @StepScope
    public SynchronizedItemStreamReader<Review> partitionReader(@Value("#{stepExecutionContext['fileName']}")String fileName) throws MalformedURLException {
        var reader = new JsonItemReaderBuilder<Review>()
                .jsonObjectReader(new JacksonJsonObjectReader<>(Review.class))
                .resource(new UrlResource(fileName))
                .name("reader")
                .build();
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formattedDateTime = now.format(formatter);
        System.out.println(formattedDateTime + " " + fileName + " thread id: " + Thread.currentThread().getId());
        return new SynchronizedItemStreamReaderBuilder<Review>()
                .delegate(reader)
                .build();
    }

    @Bean
    public TaskExecutor mainTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(16);
        executor.setMaxPoolSize(16);
        executor.setQueueCapacity(20);
        return executor;
    }

    @Bean
    public TaskExecutor subTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(16);
        return executor;
    }
}