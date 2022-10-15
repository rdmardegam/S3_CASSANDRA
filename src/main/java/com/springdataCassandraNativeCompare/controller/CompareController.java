package com.springdataCassandraNativeCompare.controller;


import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.format.annotation.DateTimeFormat.ISO;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.springdataCassandraNativeCompare.cassandraNative.CassandraNativeRepository;
import com.springdataCassandraNativeCompare.service.DummyService;
import com.springdataCassandraNativeCompare.springData.SpringDataRepository;
import com.springdataCassandraNativeCompare.springData.entity.DummyItem;

import lombok.extern.log4j.Log4j2;


/**
 * @author Ramon Mardegam
 */

@RestController
@Log4j2
public class CompareController {

    @Autowired
    SpringDataRepository springDataRepository;

    @Autowired
    CassandraNativeRepository cassandraNativeRepository;
    
    @Autowired
    DummyService dummyService;
    
    /*
    private final Bucket bucket;
    public CompareController() {
    	  Bandwidth limit = Bandwidth.classic(400, Refill.greedy(400, Duration.ofMinutes(1)));
          this.bucket = Bucket4j.builder()
              .addLimit(limit)
              .build();
    }*/
    

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/select/springdata", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> selectSpringData() {
        long startTime = System.currentTimeMillis();
        List<DummyItem> response = springDataRepository.selectAll();
        long finishTime = System.currentTimeMillis();
        System.out.println("Elapsed:" + (finishTime - startTime) + "ms");
        
        /*return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);*/
        
        return new ResponseEntity<>(response.size(), HttpStatus.OK);
        
        /*return new ResponseEntity<>((finishTime - startTime) + "ms",
                HttpStatus.OK);*/
    }
    
    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/select/springdata2", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> selectSpringData2(
    		@RequestParam(name = "dataInicio")
    		@DateTimeFormat(iso = ISO.DATE)
    		LocalDate dateInicio,
    		@RequestParam(name = "dataFim")
    		@DateTimeFormat(iso = ISO.DATE)
    		LocalDate dateFim) {
    	
        long startTime = System.currentTimeMillis();
        
        List<DummyItem> response =  Collections.synchronizedList(new ArrayList<DummyItem>());
        
        
        //IntStream.range(0,12).parallel().forEach(v -> {
        List.of(1,2,3,4,5,6,7,8,9,10,11,12).parallelStream().forEach( v -> {
        	System.out.println(v);
        	response.addAll(springDataRepository.selectAll2("35672952844", (v*10000)+2022, dateInicio, dateFim) );
        });
        
        long finishTime = System.currentTimeMillis();
        System.out.println("Elapsed:" + (finishTime - startTime) + "ms");
        
        /*return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);*/
        
        return new ResponseEntity<>(response.size(), HttpStatus.OK);
        
        /*return new ResponseEntity<>((finishTime - startTime) + "ms",
                HttpStatus.OK);*/
    }

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/select/cassandraNative", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> selectCassandraNative() {
        long startTime = System.currentTimeMillis();
        
        //List<DummyItem> response = new ArrayList<DummyItem>();
        //List<DummyItem> response =  Collections.synchronizedList(new ArrayList<DummyItem>());
        
        //response.addAll(cassandraNativeRepository.selectAll());
        //response.addAll(cassandraNativeRepository.selectAll());
        //response.addAll(cassandraNativeRepository.selectAll());
        
        // Faz 3 chaamdas em paralelo
//        List.of(1,2,3).parallelStream().forEach( v -> {
//        //List.of(1,2,3,4,5,6,7,9,10,11,12).parallelStream().forEach( v -> {
//        	System.out.println(v);
//        	response.addAll(cassandraNativeRepository.selectAll());
//        });
        
        List<DummyItem> response = List.of(1, 2, 3).parallelStream().map(v -> {
			System.out.println(v);
			return cassandraNativeRepository.selectAll();
		})
		.flatMap(List::stream)
        .collect(Collectors.toList());
        
        
        long finishTime = System.currentTimeMillis();
        //System.out.println();
        
        log.info("Elapsed:" + (finishTime - startTime) + "ms");
        
        /*return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);*/
        return new ResponseEntity<>((finishTime - startTime) + "ms",
                HttpStatus.OK);
        
    }
    
//    @CrossOrigin(origins = {"*"})
//    @RequestMapping(path = "/select/cassandraNative2", method = RequestMethod.GET)
//    @ResponseBody
//    public ResponseEntity<?> selectCassandraNative2(
//    		@RequestParam(name = "dataInicio")
//    		@DateTimeFormat(iso = ISO.DATE)
//    		LocalDate dateInicio,
//    		
//    		@RequestParam(name = "dataFim")
//    		@DateTimeFormat(iso = ISO.DATE)
//    		LocalDate dateFim
//    		
//    		) {
//        long startTime = System.currentTimeMillis();
//        List<DummyItem> response =  Collections.synchronizedList(new ArrayList<DummyItem>());
//        
//        // Faz 3 chaamdas em paralelo
//        List.of(1,2,3,4,5,6,7,9,10,11,12).parallelStream().forEach( v -> {
//        	//System.out.println(v);
//        	LocalDate start = LocalDate.of(2022, v, 1);
//        	LocalDate end = start.withDayOfMonth(start.lengthOfMonth());
//        	response.addAll(cassandraNativeRepository.selectFilter("35672952844", (v*10000)+2022,start,end));
//        	//response.addAll(cassandraNativeRepository.selectFilter("35672952844", (v*10000)+2022,dateInicio,dateFim));
//        });
//        
//        long finishTime = System.currentTimeMillis();
//        
//        log.info("Elapsed:" + (finishTime - startTime) + "ms");
//
//        //return new ResponseEntity<>(response.size(), HttpStatus.OK);
//        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms", HttpStatus.OK);
//        
//    }
    

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/select/cassandraNative2", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> selectCassandraNative2(
    		@RequestParam(name = "dataInicio")
    		@DateTimeFormat(iso = ISO.DATE)
    		LocalDate dateInicio,
    		
    		@RequestParam(name = "dataFim")
    		@DateTimeFormat(iso = ISO.DATE)
    		LocalDate dateFim) {
        
    	long startTime = System.currentTimeMillis();
    	
    	/*
    	System.out.println("---- TOKEN QNT "+ bucket.getAvailableTokens());
    	boolean processaParalelo =  bucket.tryConsume(1);
    	*/
    	
    	
    	List<DummyItem> list = dummyService.selectFilter(null, dateInicio, dateFim,true);

    	System.out.println("Elapsed:" + (System.currentTimeMillis() - startTime) + "ms");

        //return new ResponseEntity<>(response.size(), HttpStatus.OK);
        return new ResponseEntity<>(list.size() /*+response.size()*/, HttpStatus.OK);
        
    }
    
    

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/insert/springdata/{count}", method = RequestMethod.GET)
    @ResponseBody 
    public ResponseEntity<?> insertSpringData(@PathVariable("count") long count) {
        long startTime = System.currentTimeMillis();
        
        LongStream.range(0,count).parallel().forEach(i-> { 
        	
        	String cpf = List.of("35672952844","11111111111", "22222222222").get(new Random().nextInt(3));
        	
        	int mes  = new Random().nextInt(12) + 1;
        	Integer mesAno = Integer.parseInt(Integer.toString(mes) + "2022");
        	String chave = UUID.randomUUID().toString();
        	
        	LocalDate data =  LocalDate.of(2022, mes, new Random().nextInt(26)+1); 
        	String codigoMoeda = "R$";
        	double valor = 0 + (1000 - 0) * new Random().nextDouble();
        	
        	springDataRepository.insert(cpf, mesAno, chave, data, codigoMoeda, valor);
        	
        });
        
        long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);
    }

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/select/cassandraNative/{count}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> insertCassandraNative(@PathVariable("count") long count) {

        long startTime = System.currentTimeMillis();
        LongStream.range(0,count).forEach(i-> cassandraNativeRepository.insertAll(i));
        long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);
    }
    
    
    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/select/cassandraNativeAsync/{count}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> cassandraNativeAsync(@PathVariable("count") long count) {
    	
    	System.out.println("THREAD PRINCIPAL - " + Thread.currentThread().getName());
    	
    	
    	 List<CompletableFuture<Void>> futures=new ArrayList<>();
         List<Long> successes = new ArrayList<>();
         List<Long> failures = Collections.synchronizedList(new ArrayList<Long>());
    	
        long startTime = System.currentTimeMillis();
        LongStream.range(0,count)/*.parallel()*/.forEach(i-> {
        	
        	//try {
				
				CompletableFuture<Void> completableFuture = cassandraNativeRepository.insertAsync(i).handle(((integer, ex) -> {
	                if (ex != null) {
	                    failures.add(i);
	                } else {
	                    successes.add(i);
	                }
	                return integer;
	            }));
				
				futures.add(completableFuture);
				
			/*} catch (Exception e) {
				System.out.println("ERRORRRRRRR" + e.getMessage());
			}*/
        	//System.out.println("PASSOU " + i);
        	
        });
        
        
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        System.out.println("successes: "+ successes);
        System.out.println("failures: "+ failures);
        System.out.println("TOTAL failures: "+ failures.size());
        
        
        long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);
    }


    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/update/springdata/{count}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> updateSpringData(@PathVariable("count") long count) {

        long startTime = System.currentTimeMillis();
        LongStream.range(0,count).forEach(i-> springDataRepository.update(i, "FYildizli", "fatih"));
        long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);
    }

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/update/cassandraNative/{count}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> updateCassandraNative(@PathVariable("count") long count) {

        long startTime = System.currentTimeMillis();
        LongStream.range(0,count).forEach(i-> cassandraNativeRepository.updateAll(i));
        long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);
    }

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/delete/springdata/{count}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> deleteSpringData(@PathVariable("count") long count) {

        long startTime = System.currentTimeMillis();
        LongStream.range(0,count).forEach(i-> springDataRepository.delete(i));
        long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);
    }

    @CrossOrigin(origins = {"*"})
    @RequestMapping(path = "/delete/cassandraNative/{count}", method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<?> deleteCassandraNative(@PathVariable("count") long count) {

        long startTime = System.currentTimeMillis();
        LongStream.range(0,count).forEach(i-> cassandraNativeRepository.deleteAll(i));
        long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms",
                HttpStatus.OK);
    }

}
