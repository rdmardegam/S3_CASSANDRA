package com.springdataCassandraNativeCompare.cassandraNative.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

@Configuration
public class WebClientConfig {
 
	// https://www.dhaval-shah.com/performant-and-optimal-spring-webclient/
	
	@Bean
	public WebClient webClientLoad(WebClient.Builder builder) {
		
		ConnectionProvider connProvider = ConnectionProvider
				                                    .builder("webclient-conn-pool")
				                                    .maxConnections(500)
				                                    //.maxIdleTime()
				                                    //.maxLifeTime()
				                                    //.pendingAcquireMaxCount()
				                                    //.pendingAcquireTimeout(Duration.ofMillis(acquireTimeoutMillis))
				                                    .build();

		
		HttpClient nettyHttpClient = HttpClient.create(connProvider)
				// .secure(sslContextSpec ->
				// sslContextSpec.sslContext(webClientSslHelper.getSslContext()))
				.tcpConfiguration(tcpClient -> {
					LoopResources loop = LoopResources.create("webclient-event-loop");
					// ,selectorThreadCount, workerThreadCount, Boolean.TRUE);

					return tcpClient.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1200)
							.option(ChannelOption.TCP_NODELAY, true).doOnConnected(connection -> {
								connection.addHandlerLast(new ReadTimeoutHandler(1200))
										.addHandlerLast(new WriteTimeoutHandler(1200));
							}).runOn(loop);
				});
		// .keepAlive(keepAlive)
		// .wiretap(Boolean.TRUE);

		ClientHttpConnector connector = new ReactorClientHttpConnector(nettyHttpClient);

		return WebClient.builder().clientConnector(connector).build();
		
	}
	
}
