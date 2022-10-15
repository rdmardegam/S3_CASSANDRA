package com.springdataCassandraNativeCompare.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.springdataCassandraNativeCompare.cassandraNative.CassandraNativeRepository;
import com.springdataCassandraNativeCompare.springData.entity.DummyItem;

@Service
public class ProcessaFileService {

	@Autowired
    private S3Service s3Service;
	
	@Autowired
    private CassandraNativeRepository repository;
	
	
	public void processaArquivoS3(String bucketName, String file) {
		System.out.println("#### INICIANDO PROCESSAMENTO #### ");
		
		S3Object s3object = s3Service.getS3Object(bucketName, file);;
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        
        long fileLenght = s3object.getObjectMetadata().getContentLength();
        
        //List<DummyItem> successes = new ArrayList<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<DummyItem> failures = Collections.synchronizedList(new ArrayList<DummyItem>());
        
        AtomicLong fileLine = new AtomicLong(0L);
        AtomicLong fileReadLenght = new AtomicLong(0L);
        
        //String line;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        	
        	List<DummyItem> listDummyItems = new ArrayList<DummyItem>();
        	
        	// read line by line
            //while ((line = br.readLine()) != null) {
        	br.lines().forEach(line -> {
        		
        		//Add o valor lido
        		fileReadLenght.addAndGet((long)line.length());
        		fileLine.incrementAndGet();
            	 
        		//Add os itens
                listDummyItems.add(this.convertToDummyItem(line));
                
                //
                if(listDummyItems.size() == 50000) {
                	
                	this.writeValues(listDummyItems, futures, failures, fileLine, bucketName, file);
                	
                	// Finalizou as acoes assincronas
                	System.out.println(" ***** Linha = "+ fileLine + " Bytes Lidos ="+fileReadLenght +" **********");
                	System.out.println(" ***** Percentual Lido: " + String.format("%.2f",(fileReadLenght.get()*100f)/fileLenght)+"% **********");
                }
                
                /***/
                //repository.insertAsync(item);
                
                /*if(fileLine%1000 == 0) {
                	System.out.println(" ***** Linha = "+ fileLine + " Bytes Lidos ="+fileReadLenght +" **********");
                	System.out.println(" ***** Percentual Lido: " + String.format("%.2f",(fileReadLenght*100f)/fileLenght)+"% **********");
                }*/
                
        	});
        
        	if(!listDummyItems.isEmpty()) {
        		this.writeValues(listDummyItems, futures, failures, fileLine, bucketName, file);
        		System.out.println(" ***** Linha = "+ fileLine + " Bytes Lidos ="+fileReadLenght +" **********");
            	System.out.println(" ***** Percentual Lido: " + String.format("%.2f",(fileReadLenght.get()*100f)/fileLenght)+"% **********");
        	}
        	
        	System.out.println("SOBRA DA LISTA:" + listDummyItems.size());
        	
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
        
        // Grava de forma assincrona as linhas que falharam 
        
        
        
        //CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        //System.out.println("successes: "+ successes);
        //System.out.println("failures: "+ failures);
        System.out.println("TOTAL failures: "+ failures.size());
        
        if(!failures.isEmpty()) {
        	System.out.println("ENVIANDO ARQUIVO A SER GRAVADO");
        	System.out.println(failures.stream().collect(Collectors.toMap(DummyItem::getCod_chav_lancamento, p -> p, (p, q) -> p)).values().size());
        }
        
        //failures.stream().distinct(p -> p.getCod_chav_lancamento());
        ;
        
        System.out.println("#### FINALIZANDO PROCESSAMENTO #### ");
    }

	
	private void writeValues(List<DummyItem> listDummyItems, List<CompletableFuture<Void>> futures,  List<DummyItem> failures, AtomicLong fileLine, String bucketName, String file) {
		listDummyItems.forEach(item -> {
    		/**BLOCO INSERT**/
			CompletableFuture<Void> completableFuture = repository.insertAsync(item).handle(((valor, ex) -> {
                if (ex != null) {
                	System.out.println(ex.getMessage());
                	failures.add(item);
                }
                return valor;
            }));
            
			futures.add(completableFuture);
    	});
    	
    	CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    	
    	futures.clear();
    	listDummyItems.clear();
    	
    	
    	// envia linha atual lida para o s3
    	Map<String, String> mapTag = new HashMap<String, String>();
    	mapTag.put("lineRead", Long.toString(fileLine.get()));
    	s3Service.addTagInFile(bucketName, file, mapTag);
	}
	

	private DummyItem convertToDummyItem(String line) {
		 String values[] = line.split("\\|");
         
         DummyItem item = new DummyItem();
         item.setNum_cpf_cnpj(values[0]);
         item.setMes_ano_lancamento(Integer.parseInt(values[1]));
         
         String data[] = values[2].split("-");
         item.setDat_autr(LocalDate.of(Integer.parseInt(data[0]),Integer.parseInt(data[1]),Integer.parseInt(data[2])));
         
         item.setCod_chav_lancamento(values[3]);
         
         item.setCodigo_moeda_origem(values[4]);
         item.setValor(Double.parseDouble(values[5]));
         
         return item;
	}	
	
}
