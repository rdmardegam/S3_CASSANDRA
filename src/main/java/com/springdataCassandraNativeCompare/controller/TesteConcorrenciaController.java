package com.springdataCassandraNativeCompare.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.springdataCassandraNativeCompare.service.ProcessaFileService;
import com.springdataCassandraNativeCompare.service.TesteConcorrenciaService;

@Controller
public class TesteConcorrenciaController {

	
	@Autowired
	TesteConcorrenciaService testeConcorrenciaService;
	
	@Autowired
	ProcessaFileService processaFileService;
	
	
	@RequestMapping(path = "/testee/{cpf}", method = RequestMethod.GET)
	public ResponseEntity<?> teste(@PathVariable("cpf") String cpf) {
		 System.out.println("ENTROU");
		 
		 String valorCpfRetornado = testeConcorrenciaService.testeConcorrencia(cpf);
		 
		 return new ResponseEntity<>(valorCpfRetornado, HttpStatus.OK);
	}
	
	
	
	@RequestMapping(path = "/executa", method = RequestMethod.GET)
	public ResponseEntity<?> processaArquivoS3() {
		long startTime = System.currentTimeMillis();
		
		//processaFileService.processaArquivoS3("mybucket", "arquivo_ee8ce28a-079e-4fbf-8243-e0006c0e2cdc.txt");
		
		processaFileService.processaArquivoS3("mybucket", "arquivo_0bafd5ad-ff11-4394-bd38-f3b599f5dd59.txt");
		
		long finishTime = System.currentTimeMillis();
        return new ResponseEntity<>("Elapsed:" + (finishTime - startTime) + "ms", HttpStatus.OK);
	}
	
}
