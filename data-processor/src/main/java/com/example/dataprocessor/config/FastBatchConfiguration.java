package com.example.dataprocessor.config;

import com.example.dataprocessor.model.Product;
import com.example.dataprocessor.model.Review;
import com.example.dataprocessor.service.*;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;

@Configuration
//@EnableBatchProcessing // adding it causes the job not to run in spring 3
public class FastBatchConfiguration {

    //    @Bean
    public SynchronizedItemStreamReader<Product> productReader(String filename) {
        var reader = new JsonItemReaderBuilder<Product>()
                .jsonObjectReader(new JacksonJsonObjectReader<>(Product.class))
                .resource(new ClassPathResource(filename))
                .name("reader")
                .build();
        return new SynchronizedItemStreamReaderBuilder<Product>()
                .delegate(reader)
                .build();
    }

    //    @Bean
    public SynchronizedItemStreamReader<Review> reviewReader(String filename) {
        var reader = new JsonItemReaderBuilder<Review>()
                .jsonObjectReader(new JacksonJsonObjectReader<>(Review.class))
                .resource(new ClassPathResource(filename))
                .name("reader")
                .build();
        return new SynchronizedItemStreamReaderBuilder<Review>()
                .delegate(reader)
                .build();
    }

    @Bean
    public DataSourceTransactionManager dataSourceTransactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    //    @Bean
//    public SynchronizedItemStreamWriter<Product> productWriter(DataSource dataSource) {
//        return new SynchronizedItemStreamWriterBuilder<Product>()
//                .delegate(new ProductWriter(dataSource))
//                .build();
//    }
//
//    @Bean
//    public SynchronizedItemStreamWriter<Review> reviewWriter(DataSource dataSource) {
//        return new SynchronizedItemStreamWriterBuilder<Review>()
//                .delegate(new ReviewWriter(dataSource))
//                .build();
//    }

    @Bean
    public ProductWriter productWriter(DataSource dataSource) {
        return new ProductWriter(dataSource);
    }

    @Bean
    public ReviewWriter reviewWriter(DataSource dataSource) {
        return new ReviewWriter(dataSource);
    }

    @Bean
    public ReviewUrlChecker reviewUrlChecker(DataSource dataSource) {
        return new ReviewUrlChecker(dataSource);
    }

    @Bean
    public ProductUrlChecker productUrlChecker(DataSource dataSource) {
        return new ProductUrlChecker(dataSource);
    }

    @Bean
    public Step step1(JobRepository jobRepository,
                      DataSourceTransactionManager dataSourceTransactionManager,
                      ProductWriter productWriter,
                      TaskExecutor mainTaskExecutor) {
        return new StepBuilder("step1", jobRepository)
                .<Product, Product>chunk(500, dataSourceTransactionManager)
                .reader(productReader("meta_Health_and_Personal_Care_1.json"))
                .processor(item -> item)
                .writer(productWriter)
                .taskExecutor(mainTaskExecutor)
                .build();
    }

    @Bean
    public Step step2(JobRepository jobRepository,
                      DataSourceTransactionManager dataSourceTransactionManager,
                      ReviewWriter reviewWriter,
                      TaskExecutor mainTaskExecutor) {
        return new StepBuilder("step2", jobRepository)
                .<Review, Review>chunk(500, dataSourceTransactionManager)
                .reader(reviewReader("Health_and_Personal_Care_1.json"))
                .processor(item -> item)
                .writer(reviewWriter)
                .taskExecutor(mainTaskExecutor)
                .build();
    }

    @Bean
    public Step step3(JobRepository jobRepository,
                      DataSourceTransactionManager dataSourceTransactionManager,
                      ReviewWriter reviewWriter,
                      TaskExecutor mainTaskExecutor) {
        return new StepBuilder("step3", jobRepository)
                .<Review, Review>chunk(500, dataSourceTransactionManager)
                .reader(reviewReader("Health_and_Personal_Care_2.json"))
                .processor(item -> item)
                .writer(reviewWriter)
                .taskExecutor(mainTaskExecutor)
                .build();
    }

    @Bean
    public Step step4(JobRepository jobRepository,
                      DataSourceTransactionManager dataSourceTransactionManager,
                      ReviewWriter reviewWriter,
                      TaskExecutor mainTaskExecutor) {
        return new StepBuilder("step4", jobRepository)
                .<Review, Review>chunk(500, dataSourceTransactionManager)
                .reader(reviewReader("Health_and_Personal_Care_3.json"))
                .processor(item -> item)
                .writer(reviewWriter)
                .taskExecutor(mainTaskExecutor)
                .build();
    }

    @Bean
    public Step step5(JobRepository jobRepository,
                      DataSourceTransactionManager dataSourceTransactionManager,
                      ReviewWriter reviewWriter,
                      TaskExecutor mainTaskExecutor) {
        return new StepBuilder("step5", jobRepository)
                .<Review, Review>chunk(500, dataSourceTransactionManager)
                .reader(reviewReader("Health_and_Personal_Care_4.json"))
                .processor(item -> item)
                .writer(reviewWriter)
                .taskExecutor(mainTaskExecutor)
                .build();
    }

    @Bean
    public Step step6(JobRepository jobRepository,
                      DataSourceTransactionManager dataSourceTransactionManager,
                      ReviewWriter reviewWriter,
                      TaskExecutor mainTaskExecutor) {
        return new StepBuilder("step6", jobRepository)
                .<Review, Review>chunk(500, dataSourceTransactionManager)
                .reader(reviewReader("Health_and_Personal_Care_5.json"))
                .processor(item -> item)
                .writer(reviewWriter)
                .taskExecutor(mainTaskExecutor)
                .build();
    }

    @Bean
    public Step step7(JobRepository jobRepository,
                      DataSourceTransactionManager dataSourceTransactionManager,
                      ReviewWriter reviewWriter,
                      TaskExecutor mainTaskExecutor) {
        return new StepBuilder("step7", jobRepository)
                .<Review, Review>chunk(500, dataSourceTransactionManager)
                .reader(reviewReader("Health_and_Personal_Care_6.json"))
                .processor(item -> item)
                .writer(reviewWriter)
                .taskExecutor(mainTaskExecutor)
                .build();
    }

    @Bean
    public Step step8(JobRepository jobRepository,
                      DataSourceTransactionManager dataSourceTransactionManager,
                      ReviewWriter reviewWriter,
                      TaskExecutor mainTaskExecutor) {
        return new StepBuilder("step8", jobRepository)
                .<Review, Review>chunk(500, dataSourceTransactionManager)
                .reader(reviewReader("Health_and_Personal_Care_7.json"))
                .processor(item -> item)
                .writer(reviewWriter)
                .taskExecutor(mainTaskExecutor)
                .build();
    }

    @Bean
    public Job productJob(JobRepository jobRepository, Step step1, Step step2, Step step3, Step step4,
                          Step step5, Step step6, Step step7, Step step8,  TaskExecutor subTaskExecutor) {
        return new JobBuilder("productJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(new JobListener())
                .start(new FlowBuilder<SimpleFlow>("splitflow")
                        .split(subTaskExecutor)
                        .add(new FlowBuilder<SimpleFlow>("flow1")
                                        .start(step1)
                                        .build(),
                                new FlowBuilder<SimpleFlow>("flow2")
                                        .start(step2)
                                        .build(),
                                new FlowBuilder<SimpleFlow>("flow3")
                                        .start(step3)
                                        .build(),
                                new FlowBuilder<SimpleFlow>("flow4")
                                        .start(step4)
                                        .build(),
                                new FlowBuilder<SimpleFlow>("flow5")
                                        .start(step5)
                                        .build(),
                                new FlowBuilder<SimpleFlow>("flow6")
                                        .start(step6)
                                        .build(),
                                new FlowBuilder<SimpleFlow>("flow7")
                                        .start(step7)
                                        .build(),
                                new FlowBuilder<SimpleFlow>("flow8")
                                        .start(step8)
                                        .build())
                        .build())
                .build()
                .build();
    }

    @Bean
    public TaskExecutor subTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(8);
        executor.setMaxPoolSize(16);
        executor.setQueueCapacity(20);
        return executor;
    }

    @Bean
    public TaskExecutor mainTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(8);
        executor.setMaxPoolSize(16);
        executor.setQueueCapacity(20);
        return executor;
    }
}
