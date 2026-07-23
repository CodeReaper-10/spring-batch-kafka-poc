package com.adipta.batchkafka.configuration;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.EnableJdbcJobRepository;
import org.springframework.batch.core.launch.support.JobOperatorFactoryBean;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;

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

    private final DataSource dataSource;

    public BatchInfrastructureConfig(DataSource dataSource) {
        this.dataSource = dataSource;
        ensureBatchSchema();
    }

    private void ensureBatchSchema() {
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getTables(null, null, "BATCH_JOB_INSTANCE", new String[]{"TABLE"})) {
                if (rs.next()) {
                    return;
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not check Spring Batch schema", e);
        }

        ResourceDatabasePopulator populator =
                new ResourceDatabasePopulator(new ClassPathResource("org/springframework/batch/core/schema-mysql.sql"));
        DatabasePopulatorUtils.execute(populator, dataSource);
    }

    @Bean
    public JobOperatorFactoryBean jobOperator(JobRepository jobRepository) {
        JobOperatorFactoryBean factoryBean = new JobOperatorFactoryBean();
        factoryBean.setJobRepository(jobRepository);
        factoryBean.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return factoryBean;
    }

    @Bean
    public Clock clock() {
        return Clock.systemDefaultZone();
    }
}