package com.springdataCassandraNativeCompare.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.amazonaws.services.s3.model.Tag;

public class S3Dto {

	private String bucket;
	private String file;
	private Date dateLastModification;

	private String id_reservation;	
	// long nextExecution = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
	private Long expira_reserva;

	private List<Tag> listTag; 
	
	public String getBucket() {
		return bucket;
	}
	public void setBucket(String bucket) {
		this.bucket = bucket;
	}
	public String getFile() {
		return file;
	}
	public void setFile(String file) {
		this.file = file;
	}
	public Date getDateLastModification() {
		return dateLastModification;
	}
	public void setDateLastModification(Date dateLastModification) {
		this.dateLastModification = dateLastModification;
	}
	public String getId_reservation() {
		return id_reservation;
	}
	public void setId_reservation(String id_reservation) {
		this.id_reservation = id_reservation;
	}
	public Long getExpira_reserva() {
		return expira_reserva;
	}
	public void setExpira_reserva(Long expira_reserva) {
		this.expira_reserva = expira_reserva;
	}
	public List<Tag> getListTag() {
		if(listTag ==null) {
			listTag = new ArrayList<Tag>();
		}
		return listTag;
	}
	public void setListTag(List<Tag> listTag) {
		this.listTag = listTag;
	}
	
	
	
	
}