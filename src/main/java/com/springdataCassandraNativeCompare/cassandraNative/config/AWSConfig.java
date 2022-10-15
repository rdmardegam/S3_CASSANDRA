package com.springdataCassandraNativeCompare.cassandraNative.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

@Configuration
public class AWSConfig {

//    @Bean
//    public AmazonS3 amazonS3() {
//        AmazonS3 s3client = AmazonS3ClientBuilder
//                .standard()
//                .withCredentials(new AWSStaticCredentialsProvider(credentials()))
//                //.withRegion(Regions.SA_EAST_1)
//                .withEndpointConfiguration(getEndpointConfiguration())
//                .build();
//        return s3client;
//    }
//    
//    public AWSCredentials credentials() {
//        AWSCredentials credentials = new BasicAWSCredentials(
//                "1234",
//                "1234"
//        );
//        return credentials;
//    }
//    
//    private EndpointConfiguration getEndpointConfiguration() {
//        return new EndpointConfiguration("http://localhost:4566", Regions.SA_EAST_1.name());
//    }
	
	  @Bean
	    public AmazonS3Client amazonS3() {
		  AmazonS3Client s3client = (AmazonS3Client)  AmazonS3ClientBuilder
	                .standard()
	                .withCredentials(new AWSStaticCredentialsProvider(credentials()))
	                //.withRegion(Regions.SA_EAST_1)
	                .withEndpointConfiguration(getEndpointConfiguration())
	                .build();
	        return s3client;
	    }
	    
	    public AWSCredentials credentials() {
	        AWSCredentials credentials = new BasicAWSCredentials(
	                "1234",
	                "1234"
	        );
	        return credentials;
	    }
	    
	    private EndpointConfiguration getEndpointConfiguration() {
	        return new EndpointConfiguration("http://localhost:4566", Regions.SA_EAST_1.name());
	    }
	
}
