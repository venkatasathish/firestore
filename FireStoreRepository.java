package com.example.springcloudgcpfirestorelibrary.repository;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Repository;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.QuerySnapshot;

@Repository
public interface FireStoreRepository<T> {

	public String addDocument(T document, String collectionName);

	public void setDocumentData(T documentData, String collectionName, String documentId);

	public void addAllDocuments(List<T> documents, String collectionName);
	
	public void insertDocumentsInBatch(String collectionName, Map<String, Object> documents);
	
	public List<T> findAll(String collectionName, Class<T> t);

	public List<T> findDocumentsBasedOnQuery(ApiFuture<QuerySnapshot> querySnapshot, Class<T> t);

	public T findByDocumentId(String collectionName, String docId, Class<T> t);

	public List<T> findDocumentsInSubCollection(String collectionName, String docId, String subCollectionName,
			Class<T> t);

	public void updateDocument(String collectionName, String documentId, Map<String, Object> fieldsToBeUpdated);

	public void updateDocumentBasedOnQuery(ApiFuture<QuerySnapshot> querySnapshot, String collectionName,
			Map<String, Object> fieldsToBeUpdated);
	
	public void updateDocumentsInBatch(String collectionName, Map<String, Map<String, Object>> documents);

	public void deleteDocument(String collectionName, String documentId);

	public void deleteCollection(String collectionName, int batchSize);

	public void deleteFields(String collectionName, String documentId, List<String> fields);

}
