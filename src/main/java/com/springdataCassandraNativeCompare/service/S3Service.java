package com.springdataCassandraNativeCompare.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;

import lombok.extern.log4j.Log4j2;

@Service
@Log4j2
public class S3Service {

	private static final Logger LOGGER = LogManager.getLogger(S3Service.class);
	
    //private final AmazonS3 amazonS3Client;
	
	
	private static final String RESERVATION_ID = "RESERVATION_ID";
	private static final String RESERVATION_EXPIRATION = "RESERVATION_EXPIRATION";
	
    
    @Autowired
    private AmazonS3Client amazonS3Client;

    //Bucket level operations

    public void createS3Bucket(String bucketName, boolean publicBucket) {
        if(amazonS3Client.doesBucketExist("/"+bucketName)) {
            log.info("Bucket name already in use. Try another name.");
            return;
        }
        if(publicBucket) {
            amazonS3Client.createBucket(bucketName);
        } else {
            amazonS3Client.createBucket(new CreateBucketRequest(bucketName).withCannedAcl(CannedAccessControlList.Private));
        }
    }

    public List<Bucket> listBuckets(){
        return amazonS3Client.listBuckets();
    }

    public void deleteBucket(String bucketName){
        try {
            amazonS3Client.deleteBucket(bucketName);
        } catch (AmazonServiceException e) {
            log.error(e.getErrorMessage());
            return;
        }
    }

    //Object level operations
   /* public void putObject(String bucketName, BucketObjectRepresentaion representation, boolean publicObject) throws IOException {

        String objectName = representation.getObjectName();
        String objectValue = representation.getText();

        File file = new File("." + File.separator + objectName);
        FileWriter fileWriter = new FileWriter(file, false);
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.println(objectValue);
        printWriter.flush();
        printWriter.close();

        try {
            if(publicObject) {
                var putObjectRequest = new PutObjectRequest(bucketName, objectName, file).withCannedAcl(CannedAccessControlList.PublicRead);
                amazonS3Client.putObject(putObjectRequest);
            } else {
                var putObjectRequest = new PutObjectRequest(bucketName, objectName, file).withCannedAcl(CannedAccessControlList.Private);
                amazonS3Client.putObject(putObjectRequest);
            }
        } catch (Exception e){
            log.error("Some error has ocurred.");
        }

    }*/

    public List<S3ObjectSummary> listObjects(String bucketName) {
        
    	/**GET LIST FILE ON BUCKET*/
    	ObjectListing objectListing = amazonS3Client.listObjects("/"+bucketName);

        String fileName = objectListing.getObjectSummaries().get(0).getKey();
        
        
        /**GET TAGS ON FILE BUCKET**/
        GetObjectTaggingRequest getTaggingRequest = new GetObjectTaggingRequest( "/"+ bucketName, fileName);
        getTaggingRequest.toString();
        
        GetObjectTaggingResult tags = amazonS3Client.getObjectTagging(getTaggingRequest);
        List<Tag> tagSet= tags.getTagSet();

        //Iterate through the list
        Iterator<Tag> tagIterator = tagSet.iterator();
        while(tagIterator.hasNext()) {
            Tag tag = (Tag)tagIterator.next();
            //System.out.println(tag.getKey());
            //System.out.println(tag.getValue());
        }
        
        // add tags
        /*List<Tag> newTags = new ArrayList<Tag>();
        newTags.add(new Tag("Tag 3", "This is tag 3"));
        newTags.add(new Tag("Tag 4", "This is tag 4"));
        amazonS3Client.setObjectTagging(new SetObjectTaggingRequest( "/"+ bucketName, fileName, new ObjectTagging(newTags)));
        */
        
        /**GET OBJECT METADATA*/
        //GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest("/"+ bucketName, fileName);
        //ObjectMetadata metadata = amazonS3Client.getObjectMetadata(metadataRequest);

        //metadata.getRawMetadataValue(fileName)
        //System.out.println("Replication Status : " + metadata.getRawMetadataValue(Headers.OBJECT_REPLICATION_STATUS));
        
        
        
        //GetObjectTaggingResult tags = amazonS3Client.getObjectTagging(getTaggingRequest);
        
        
        
        /*ObjectMetadata metadataCopy = new ObjectMetadata();
        // copy previous metadata
        metadataCopy.addUserMetadata("newmetadata", "newmetadatavalue");*/
        
        return objectListing.getObjectSummaries();
    }
    
      
    

   

	public void downloadObject(String bucketName, String objectName){
        S3Object s3object = amazonS3Client.getObject(bucketName, objectName);
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        try {
            FileUtils.copyInputStreamToFile(inputStream, new File("." + File.separator + objectName));
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    public void deleteObject(String bucketName, String objectName){
        amazonS3Client.deleteObject(bucketName, objectName);
    }

    public void deleteMultipleObjects(String bucketName, List<String> objects){
        DeleteObjectsRequest delObjectsRequests = new DeleteObjectsRequest(bucketName)
                .withKeys(objects.toArray(new String[0]));
        amazonS3Client.deleteObjects(delObjectsRequests);
    }

    public void moveObject(String bucketSourceName, String objectName, String bucketTargetName){
        amazonS3Client.copyObject(
                bucketSourceName,
                objectName,
                bucketTargetName,
                objectName
        );
    }
    
    public List<String> generateFiles(String bucketName, int quantidade){
		//createS3Bucket(bucketName, true);
    	List<String> filesGenerate = Collections.synchronizedList(new ArrayList<String>()); 
		IntStream stream = IntStream.range(0, quantidade); 
		
		stream.parallel().forEach(arquivo-> {
			try {
				String url = null;
				String fileKey = "arquivo_" + UUID.randomUUID()+".txt";
				
				File fileTemp = File.createTempFile(fileKey, ".txt");
				
				try (BufferedWriter bw = new BufferedWriter(new FileWriter(fileTemp))) {

					// 10M
					//1.5M
					for(int x=1;x<=1500000;x++) {
						if(x!=1) {
							bw.write(System.lineSeparator()); // new line
						}
						
						String cpf = StringUtils.leftPad(Long.toString(x), 11, "0");
			            
			            String valueWrite = cpf +"|"+ 
			            		"102022" +"|"+
			            		LocalDate.now() +"|"+
			            		UUID.randomUUID().toString() +"|"+ 
			            		"R$" +"|"+
			            		Double.valueOf(x);
						
						
						bw.write(valueWrite);
						
						//Files.write(file, sbuffer.toString(), StandardOpenOption.APPEND);
						//Files.write(pathFile, linhasConteudo, StandardCharsets.UTF_8);
					}
				}
		
				// Metadados
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentLength(fileTemp.length());
				metadata.setContentType("text/plain; charset=utf-8");
				metadata.setHttpExpiresDate(null);

			LOGGER.info("Realizando upload no S3");
			amazonS3Client.putObject(new PutObjectRequest("/" + bucketName, fileKey, new FileInputStream(fileTemp), metadata)
					.withCannedAcl(CannedAccessControlList.PublicRead));
			LOGGER.info("UPLOAD REALIZADO COM SUCESSO no S3");

			// Recupera a url
			url = amazonS3Client.getResourceUrl(bucketName, fileKey);
			
			filesGenerate.add(url);
			
			LOGGER.info("Novo arquivo no S3 {}", fileTemp);
			LOGGER.info("URL Result S3: {}", url);
			
			Files.deleteIfExists(fileTemp.toPath());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
			
		});
		
    	return filesGenerate;
    }
    
    
    public List<S3Dto> listAllFilesBucket(String bucketName) {
    	/**GET LIST FILE ON BUCKET*/
    	ObjectListing objectListing = amazonS3Client.listObjects(bucketName);
    	
    	return objectListing.getObjectSummaries().stream().map(v -> {
    		
    		S3Dto s3Dto = new S3Dto();
    		s3Dto.setBucket(v.getBucketName());
    		s3Dto.setFile(v.getKey());
    		s3Dto.setDateLastModification(v.getLastModified());
    		
    		s3Dto.setExpira_reserva(0L);
    		s3Dto.setId_reservation(null);
    		
    		/**GET TAGS ON FILE BUCKET**/
            GetObjectTaggingRequest getTaggingRequest = new GetObjectTaggingRequest(bucketName, v.getKey());
            GetObjectTaggingResult tags = amazonS3Client.getObjectTagging(getTaggingRequest);
			
            // Filtra apenas pelas tags que deseja
            List<Tag> tagSet = tags.getTagSet().stream().
							   filter(t -> t.getKey().equals(RESERVATION_ID) || t.getKey().equals(RESERVATION_EXPIRATION) )
							   .collect(Collectors.toList());

            // Atribui as tags no objeto referencia
			tagSet.forEach(tag -> {
				if(tag.getKey().equals(RESERVATION_ID)) {
					s3Dto.setId_reservation(tag.getValue());
				}else if(tag.getKey().equals(RESERVATION_EXPIRATION)) {
					s3Dto.setExpira_reserva(Long.valueOf(tag.getValue()));
				}
			});
			
			
			s3Dto.setListTag(tags.getTagSet());	
			
            //Iterate through the list
            /*Iterator<Tag> tagIterator = tagSet.iterator();
            while(tagIterator.hasNext()) {
                Tag tag = (Tag)tagIterator.next();
                //System.out.println(tag.getKey());
                //System.out.println(tag.getValue());
            }*/
    		
    		return s3Dto;
    		
    	}).collect(Collectors.toList());
	}
    
    public List<S3Dto> listAvaiableFileToReservation(String bucketName) {
    	bucketName = "/"+bucketName;
    	
    	List<S3Dto> listS3 = this.listAllFilesBucket(bucketName);
    	
    	// Retorna apenas arquivos sem reserva ou que tenha a data de reserva expirada
		return listS3.stream().
					  filter(file -> (System.currentTimeMillis()/1000) > file.getExpira_reserva())
					  .collect(Collectors.toList());
    }
    
  
    
    public List<S3Dto> listMyReservation(String bucketName, String idProgramExecution) {
    	bucketName = "/"+bucketName;
    	
    	List<S3Dto> listS3 = this.listAllFilesBucket(bucketName);

    	// Retorna apenas reservas do id
		return listS3.stream().
					  filter(file -> {
						  return idProgramExecution.equals(file.getId_reservation()) &&
							  	     (System.currentTimeMillis()/1000) <= file.getExpira_reserva();
						  
					  } )
					  .collect(Collectors.toList());
		
	}
    
    
    
    
    //
    public boolean reserveFile(String bucketName, String idReservation, long tsExpirationReservation) {
    	boolean reserved = false;
    	
    	List<S3Dto> list = listAvaiableFileToReservation(bucketName);
    	
    	if(list.size() >0) {
    		// Embaralha lista
    		Collections.shuffle(list);
    		if( list.size() > 3 ) {
    			list =  list.subList(0, 3);
    		}

    		// Aplica tag para reservar
    		List<Tag> newTags = new ArrayList<Tag>();
            newTags.add(new Tag(RESERVATION_ID, idReservation));
            newTags.add(new Tag(RESERVATION_EXPIRATION, Long.toString(  (System.currentTimeMillis() + tsExpirationReservation) /1000)) );
            
            list.forEach(e-> amazonS3Client.setObjectTagging(new SetObjectTaggingRequest( "/"+ bucketName, e.getFile(), new ObjectTagging(newTags))));
            reserved = true;
    	}
    	
    	return reserved;
    }
    
    public boolean zeraReserva(String bucketName) {
    	boolean reserved = false;
    	String bucketNameAx = "/"+bucketName;
    	
    	List<S3Dto> list = listAllFilesBucket(bucketNameAx);
    	
    	// Aplica tag para reservar
		List<Tag> newTags = new ArrayList<Tag>();
        newTags.add(new Tag(RESERVATION_ID, "0"));
        newTags.add(new Tag(RESERVATION_EXPIRATION, "0"));
        
        for(S3Dto s3Data : list) {
        	//new DeleteObjectTaggingRequest(bucketNameAx, bucketNameAx)
        	amazonS3Client.setObjectTagging(new SetObjectTaggingRequest(bucketNameAx, s3Data.getFile(), new ObjectTagging(newTags)));
        	reserved = true;
        }
        
    	return reserved;
    }
    
    public void addTagInFile(String bucketName, String fileName, Map<String,String> tagMap) {
    	
		List<Tag> newTags = tagMap.entrySet().stream()
						    .map(m -> new Tag(m.getKey(), m.getValue()))
						    .collect(Collectors.toList());
    	
    	amazonS3Client.setObjectTagging(new SetObjectTaggingRequest("/"+bucketName, fileName, new ObjectTagging(newTags)));
    }
    

	public void processFile(String bucketName, String file) {
		S3Object s3object = amazonS3Client.getObject("/"+bucketName, file);
        S3ObjectInputStream inputStream = s3object.getObjectContent();
        
        long fileLenght = s3object.getObjectMetadata().getContentLength();
        
     // try-with-resources, auto close
        String line;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            
        	long fileLine = 0L;
        	long fileReadLenght = 0l;
        	
        	// read line by line
            while ((line = br.readLine()) != null) {
                fileLine++;
            	
            	System.out.println("Thread:"+Thread.currentThread().getId() +  " -- file:"+ file + " -- "+ line);
                
            	fileReadLenght+=line.length();
                if(fileLine%1000 == 0) {
                	System.out.println(" ***** Linha = "+ fileLine + " Bytes Lidos ="+fileReadLenght +" **********");
                	System.out.println(" ***** Percentual Lido: " + String.format("%.2f",(fileReadLenght*100f)/fileLenght)+"% **********");
                }
                
            }
        
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }	


	
	public S3Object getS3Object(String bucketName, String file) {
		return amazonS3Client.getObject("/"+bucketName, file);
	}

	
}