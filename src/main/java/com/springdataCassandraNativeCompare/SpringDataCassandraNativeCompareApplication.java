package com.springdataCassandraNativeCompare;

import java.time.Duration;
import java.util.concurrent.Executor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.datastax.oss.driver.internal.core.tracker.RequestLogger;

@SpringBootApplication
@ComponentScan({"com.springdataCassandraNativeCompare*"})
@EntityScan({"com.springdataCassandraNativeCompare.*" })
@EnableCassandraRepositories("com.springdataCassandraNativeCompare.springData")
@EnableAsync
public class SpringDataCassandraNativeCompareApplication {



    public static void main(String[] args) {
    	System.setProperty("datastax-java-driver.advanced.request-tracker.class", "RequestLogger");
    	System.setProperty("datastax-java-driver.advanced.request-tracker.logs.success.enabled", "true");
    	System.setProperty("datastax-java-driver.advanced.request-tracker.logs.error.enabled", "true");
    	System.setProperty("datastax-java-driver.advanced.request-tracker.logs.slow.enabled", "true");
    	
    	System.setProperty("datastax-java-driver.advanced.request-tracker.logs.slow.threshold ", "0.000001 seconds");
    	System.setProperty("datastax-java-driver.advanced.request-tracker.logs.slow.enabled ", "true");
    	
    	System.setProperty("datastax-java-driver.advanced.request-tracker.logs.show-values", "true");
    	System.setProperty("datastax-java-driver.advanced.request-tracker.logs.max-value-length", "100");
    	System.setProperty("datastax-java-driver.advanced.request-tracker.logs.max-values", "100");
    	System.setProperty("datastax-java-driver.advanced.request-tracker.logs.show-stack-trace", "true");

    	System.setProperty("datastax-java-driver.protocol.version", "V5"); 
    	
    	
    	System.out.println("PROCESSADORES DISPONIVEIS: " + Runtime.getRuntime().availableProcessors());
    	System.out.println("MEMORIA DISPONIVEIS: " + Runtime.getRuntime().maxMemory());
    	
    	SpringApplication.run(SpringDataCassandraNativeCompareApplication.class, args); 

    }

   /* @Bean(name = "threadPoolExecutor")
    public Executor getAsyncExecutor() {
    	ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    	executor.setCorePoolSize(20);
    	executor.setMaxPoolSize(200);
    	executor.setQueueCapacity(400);
    	executor.setThreadNamePrefix("threadPoolExecutor-");
    	executor.initialize();
    	return executor;
    }*/

    @Bean(name = "threadCassandraPoolExecutor")
    public Executor getAsyncExecutor() {
    	ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
//    	executor.setCorePoolSize(80);
//    	executor.setMaxPoolSize(160);
//    	executor.setQueueCapacity(1000000);
    	
    	executor.setCorePoolSize(400);  // Quantidade de threads sempre ativas
    	executor.setMaxPoolSize(400);  // Quantidade de threads ate onde é possivel alargar
    	executor.setQueueCapacity(2000000); // Quantidade de item na fila... apos o MaxPoolSize ser atigindo
    	//executor.setThreadNamePrefix("threadPoolExecutor-");
    	executor.initialize();
    	return executor;
    }



}
