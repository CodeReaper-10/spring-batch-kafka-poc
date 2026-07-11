package com.adipta.batchkafka.configuration;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.batch.core.launch.support.JobOperatorFactoryBean;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
@EnableBatchProcessing
@EnableJdbcJobRepository(
        dataSourceRef = "dataSource",
        transactionManagerRef = "transactionManager"
)
public class BatchInfrastructureConfig {
    // Spring Boot autoconfigures dataSource/transactionManager beans
    // from application.yml -- nothing else needed here for
    // a single-DB POC.

    @Bean
    public JobOperatorFactoryBean jobOperator(JobRepository jobRepository) {
        JobOperatorFactoryBean factoryBean = new JobOperatorFactoryBean();
        factoryBean.setJobRepository(jobRepository);
        factoryBean.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return factoryBean;
    }
}