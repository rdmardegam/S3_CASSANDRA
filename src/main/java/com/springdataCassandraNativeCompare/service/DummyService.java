package com.springdataCassandraNativeCompare.service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.springdataCassandraNativeCompare.cassandraNative.CassandraNativeRepository;
import com.springdataCassandraNativeCompare.springData.entity.DummyItem;

@Service
public class DummyService {

	
	@Autowired
	CassandraNativeRepository cassandraNativeRepository;
	
	
	
//	public List<DummyItem> selectFilter(String num_cpf_cnpj , LocalDate dataInicio, LocalDate dataFim) {
//		
////		List<DummyItem> response = new ArrayList<DummyItem>();
////		
////		CompletableFuture<List<DummyItem>> v1 = cassandraNativeRepository.selectFilterAsync("35672952844", (1 * 10000) + 2022, dataInicio, dataFim);
////		System.out.println("EXEUTA 1");
////		CompletableFuture<List<DummyItem>> v2 = cassandraNativeRepository.selectFilterAsync("35672952844", (2 * 10000) + 2022, dataInicio, dataFim);
////		System.out.println("EXEUTA 2");
////		CompletableFuture<List<DummyItem>> v3 = cassandraNativeRepository.selectFilterAsync("35672952844", (3 * 10000) + 2022, dataInicio, dataFim);
////		System.out.println("EXEUTA 3");
////
////		/*List<CompletableFuture<List<DummyItem>>> futureList = List.of(1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12).stream()
////				.map(v -> CompletableFuture.supplyAsync(() -> {
////					LocalDate start = LocalDate.of(2022, v, 1);
////					LocalDate end = start.withDayOfMonth(start.lengthOfMonth());
////					return cassandraNativeRepository.selectFilter("35672952844", (1 * 10000) + 2022, start, end);
////				})).collect(Collectors.toList());*/
////
////		System.out.println("PASSOU1");
////		
////		CompletableFuture.allOf(v1,v2,v3).join();
////		
////		System.out.println("PASSOU2");
////				
////		
////		try {
////			response.addAll(v1.get());
////			System.out.println("PASSOU3");
////			response.addAll(v2.get());
////			System.out.println("PASSOU4");
////			response.addAll(v3.get());
////			System.out.println("PASSOU5");
////		} catch (InterruptedException e) {
////			// TODO Auto-generated catch block
////			e.printStackTrace();
////		} catch (ExecutionException e) {
////			// TODO Auto-generated catch block
////			e.printStackTrace();
////		}
//		
//		
//		
//		List<CompletableFuture<List<DummyItem>>> list = new ArrayList<CompletableFuture<List<DummyItem>>>();
//		
//		list = List.of(1, 2, 3).stream()
//		.map(v -> {
//			LocalDate start = LocalDate.of(2022, v, 1);
//			LocalDate end = start.withDayOfMonth(start.lengthOfMonth());
//			return cassandraNativeRepository.selectFilterAsync("35672952844", (v * 10000) + 2022, start, end);
//		} ).collect(Collectors.toList());
//		
//		
//		System.out.println("PASSOU1");
//		
//		CompletableFuture.allOf(list.toArray(new CompletableFuture[list.size()])).join();
//		
//		System.out.println("PASSOU2");
//				
//		List<DummyItem> response = new ArrayList<DummyItem>();
//		
//		
//		response = list.stream().map(t -> {
//			try {
//				return t.get();
//			} catch (InterruptedException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			} catch (ExecutionException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
//			return null;
//		}).flatMap(List::stream).collect(Collectors.toList());
//
//		
//		return response;
//	}
	
	
	public List<DummyItem> selectFilter(String num_cpf_cnpj , LocalDate dataInicio, LocalDate dataFim, boolean processaParalelo) {
		List<DummyItem> list = null;
		
//		if(processaParalelo) {
//			System.out.println("-----------------------------------------------------PARALELO");
//			 list = List.of(1, 2, 3).parallelStream().map(v -> {
//				LocalDate start = LocalDate.of(2022, v, 1);
//				LocalDate end = start.withDayOfMonth(start.lengthOfMonth());
//				List<DummyItem> list2 = cassandraNativeRepository.selectFilter("35672952844", (v * 10000) + 2022, start, end);
//				return (list2 == null) ? new ArrayList<DummyItem>() : list2;
//			}).flatMap(Collection::stream).collect(Collectors.toList());
//		} else {
//			System.out.println("------------------------------------------------------SYNCRONO ");
//			    list = List.of(1, 2, 3).stream().map(v -> {
//				LocalDate start = LocalDate.of(2022, v, 1);
//				LocalDate end = start.withDayOfMonth(start.lengthOfMonth());
//				List<DummyItem> list2 = cassandraNativeRepository.selectFilter("35672952844", (v * 10000) + 2022, start, end);
//				return (list2 == null) ? new ArrayList<DummyItem>() : list2;
//			} ).flatMap(Collection::stream).collect(Collectors.toList());
//		}
		
		
		 list = List.of(1, 2, 3).parallelStream().map(v -> {
				LocalDate start = LocalDate.of(2022, v, 1);
				LocalDate end = start.withDayOfMonth(start.lengthOfMonth());
				return cassandraNativeRepository.selectFilter("35672952844", (v * 10000) + 2022, start, end);
			} ).flatMap(Collection::stream).collect(Collectors.toList());
		
		
		
		
		return list;
	}
	
}