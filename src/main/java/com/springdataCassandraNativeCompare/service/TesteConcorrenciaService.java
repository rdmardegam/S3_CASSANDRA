package com.springdataCassandraNativeCompare.service;

import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Service;

@Service
@Scope( proxyMode = ScopedProxyMode.TARGET_CLASS)
public class TesteConcorrenciaService {
	
	ClasseDTO objetoQualquer;
	
	public String testeConcorrencia(String cpf) {
		objetoQualquer = new ClasseDTO(cpf);
				
		try {
			//5 segundos de delay
			Thread.sleep(1000 * 5);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return objetoQualquer.getCpf();
	}
	
}