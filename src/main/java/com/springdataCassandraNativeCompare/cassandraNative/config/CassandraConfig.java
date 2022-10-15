package com.springdataCassandraNativeCompare.cassandraNative.config;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletionStage;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;

/**
 * @author Ramon Mardegam
 */
@Configuration
@ConfigurationProperties
public class CassandraConfig {


    private static final String CASSANDRA_BOOTSTRAP_SERVER_IP = "127.0.0.1";
    private static final Integer CASSANDRA_BOOTSTRAP_SERVER_PORT = 9042;
    private static final String CASSANDRA_LOCALDATACENTER = "datacenter1";

    
    
    private static CqlSessionBuilder cqlSessionBuilder() {

        CqlSessionBuilder builder = CqlSession.builder();

        builder.addContactPoint(new InetSocketAddress(CASSANDRA_BOOTSTRAP_SERVER_IP, CASSANDRA_BOOTSTRAP_SERVER_PORT));
        builder.withLocalDatacenter(CASSANDRA_LOCALDATACENTER);
        
        
        
        DriverConfigLoader loader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "ONE")
                
                //.withLong(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 3)
                //.withInt(DefaultDriverOption.METADATA_SCHEMA_REQUEST_PAGE_SIZE, 50000)
                
                //.withString(DefaultDriverOption.LOAD_BALANCING_POLICY, 3)
                
                .build();
        //DefaultLoadBalancingPolicy
        //builder.withConfigLoader(loader);

        //System.out.println("MAX EXECTION = "+ config.getDefaultProfile() DefaultDriverOption.CONNECTION_MAX_REQUESTS);
        
        
        
        
        
        return builder;
    }

    private static CompletionStage<CqlSession> cqlSessionCompletionStage = cqlSessionBuilder().buildAsync();

    public static CqlSession getCqlSession() {
        BlockingOperation.checkNotDriverThread();
        return CompletableFutures.getUninterruptibly(cqlSessionCompletionStage);
    }
}
