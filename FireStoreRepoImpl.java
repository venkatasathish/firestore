package com.example.springcloudgcpfirestorelibrary.repositoryImpl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.example.springcloudgcpfirestorelibrary.exception.FireStoreCustomException;
import com.example.springcloudgcpfirestorelibrary.repository.FireStoreRepository;
import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldValue;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.SetOptions;
import com.google.cloud.firestore.WriteBatch;

public class FireStoreRepoImpl<T> implements FireStoreRepository<T> {

	private Firestore fireStoreDb;
	private static final Logger logger = LogManager.getLogger(FireStoreRepoImpl.class);

	public FireStoreRepoImpl(Firestore firestore) {
		super();
		this.fireStoreDb = firestore;
	}

	/**
	 * Adds document to the collection in firestore using collectionName(Auto
	 * generates id)
	 * 
	 * @param document
	 * @param collectionName
	 */
	@Override
	public String addDocument(T document, String collectionName) {
		ApiFuture<DocumentReference> addedDocRef = this.fireStoreDb.collection(collectionName).add(document);
		try {
			return addedDocRef.get().getId();
		} catch (InterruptedException | ExecutionException e) {
			logger.info("Adding document failed");
			throw new FireStoreCustomException("Adding document failed due to " + e.getCause());
		}
	}

	/**
	 * Sets the document data to the collection with the passed documentId and
	 * collection Name
	 * 
	 * @param documentData
	 * @param collectionName
	 * @param documentId
	 */
	@Override
	public void setDocumentData(T documentData, String collectionName, String documentId) {
		this.fireStoreDb.collection(collectionName).document(documentId).set(documentData, SetOptions.merge());
	}

	/**
	 * Adds documents to the collection in firestore using collectionName
	 * 
	 * @param documents
	 * @param collectionName
	 */
	@Override
	public void addAllDocuments(List<T> documents, String collectionName) {
		documents.forEach(document -> addDocument(document, collectionName));
	}
	
	/**
	 * Insert Documents to the collection in batch 
	 * 
	 * @param collectionName
	 * @param documents
	 */
	@Override
	public void insertDocumentsInBatch(String collectionName, Map<String, Object> documents) {
		
		WriteBatch batch = this.fireStoreDb.batch();
		
		documents.forEach((k,v)->{
			DocumentReference docRef = this.fireStoreDb.collection(collectionName).document(k);
			batch.set(docRef,v,SetOptions.merge());
			});
		batch.commit();
	}

	/**
	 * Method to List all documents in a collection
	 * 
	 * @param collectionName
	 * @param t(The destination class you want to map the documents to)
	 */
	@Override
	public List<T> findAll(String collectionName, Class<T> t) {
		ApiFuture<QuerySnapshot> querySnapshot = this.fireStoreDb.collection(collectionName).get();
		try {
			return this.findDocumentsBasedOnQuery(querySnapshot, t);
		} catch (Exception e) {
			logger.info("Finding all documents failed");
			throw new FireStoreCustomException("Finding all documents failed due to " + e.getCause());
		}
	}

	/**
	 * Method to find all documents based on a querySnapshot in a collection
	 * 
	 * @param querySnapshot
	 * @param t(The destination class you want to map the documents to)
	 */
	@Override
	public List<T> findDocumentsBasedOnQuery(ApiFuture<QuerySnapshot> querySnapshot, Class<T> t) {
		List<QueryDocumentSnapshot> documents;
		try {
			documents = querySnapshot.get().getDocuments();
			return documents.stream().map(doc -> doc.toObject(t)).collect(Collectors.toList());
		} catch (InterruptedException | ExecutionException e) {
			logger.info("Finding all documents based on Query failed");
			throw new FireStoreCustomException("Finding Documents Based on Query failed due to " + e.getCause());
		}
	}

	/**
	 * Method to find all documents based on documentId in a collection
	 * 
	 * @param collectionName
	 * @param docId(document Id)
	 * @param t(The destination class you want to map the documents to)
	 * 
	 */
	@Override
	public T findByDocumentId(String collectionName, String docId, Class<T> t) {
		ApiFuture<DocumentSnapshot> docSnapshot = this.fireStoreDb.collection(collectionName).document(docId).get();
		try {
			return docSnapshot.get().toObject(t);
		} catch (InterruptedException | ExecutionException e) {
			logger.info("Finding Documents Based on Doc Id failed");
			throw new FireStoreCustomException("Finding Documents Based on Doc Id failed due to " + e.getCause());
		}
	}

	/**
	 * Method to find all documents in a subcollection of a document in a collection
	 * 
	 * @param collectionName
	 * @param docId(document Id)
	 * @param subCollectionName
	 * @param t(The destination class you want to map the documents to)
	 */
	@Override
	public List<T> findDocumentsInSubCollection(String collectionName, String docId, String subCollectionName,
			Class<T> t) {
		ApiFuture<QuerySnapshot> querySnapshot = this.fireStoreDb.collection(collectionName).document(docId)
				.collection(subCollectionName).get();
		try {
			return this.findDocumentsBasedOnQuery(querySnapshot, t);
		} catch (Exception e) {
			logger.info("Finding all documents in subcollection failed");
			throw new FireStoreCustomException("Finding all documents in subcollection failed due to " + e.getCause());
		}
	}

	/**
	 * Method to update fields in the document of a collection only if document
	 * exists
	 * 
	 * @param String collectionName
	 * @param String documentId
	 * @param Map<String,Object> fieldsToBeUpdated
	 */
	@Override
	public void updateDocument(String collectionName, String documentId, Map<String, Object> fieldsToBeUpdated) {
		try {
			this.fireStoreDb.collection(collectionName).document(documentId).update(fieldsToBeUpdated);
		} catch (Exception e) {
			logger.info("Updating fields in the document failed");
			throw new FireStoreCustomException("Updating fields in the document failed due to " + e.getCause());
		}
	}

	/**
	 * Method to update fields in the documents(the documents fetched based on
	 * query) of a collection
	 * 
	 * @param ApiFuture<QuerySnapshot> querySnapshot
	 * @param String collectionName
	 * @param Map<String,Object> fieldsToBeUpdated
	 */
	@Override
	public void updateDocumentBasedOnQuery(ApiFuture<QuerySnapshot> querySnapshot, String collectionName,
			Map<String, Object> fieldsToBeUpdated) {
		try {
			List<QueryDocumentSnapshot> docsBasedOnQuery = querySnapshot.get().getDocuments();
			docsBasedOnQuery.forEach(doc -> updateDocument(collectionName, doc.getId(), fieldsToBeUpdated));
		} catch (InterruptedException | ExecutionException e) {
			logger.info("Updating documents based on query failed");
			throw new FireStoreCustomException("Updating documents based on query failed due to " + e.getCause());
		}

	}
	
	
	/**
	 * Update Documents to the collection in batch 
	 * 
	 * @param collectionName
	 * @param documents( Map having key DocumentID and value with Map of fields needs to be updated )
	 */
	@Override
	public void updateDocumentsInBatch(String collectionName, Map<String, Map<String, Object>> documents) {
		WriteBatch batch = this.fireStoreDb.batch();
		documents.forEach((k,v)->{
			DocumentReference docRef = this.fireStoreDb.collection(collectionName).document(k);
			batch.update(docRef,v);
			});
		batch.commit();
	}


	/**
	 * Delete a document in a collection.
	 *
	 * @param collectionName
	 * @param documentId
	 */
	public void deleteDocument(String collectionName, String documentId) {
		this.fireStoreDb.collection(collectionName).document(documentId).delete();
	}

	/**
	 * Delete a collection in batches to avoid out-of-memory errors. Batch size may
	 * be tuned based on document size (atmost 1MB) and application requirements.
	 * 
	 * @throws FireStoreCustomException
	 *
	 * @param collectionName
	 * @param batchSize
	 */
	public void deleteCollection(String collectionName, int batchSize) {
		try {

			CollectionReference collection = this.fireStoreDb.collection(collectionName);
			// retrieve a small batch of documents to avoid out-of-memory errors
			ApiFuture<QuerySnapshot> future = collection.limit(batchSize).get();
			int deleted = 0;

			// future.get() blocks on document retrieval
			List<QueryDocumentSnapshot> documents = future.get().getDocuments();
			for (QueryDocumentSnapshot document : documents) {
				document.getReference().delete();
				++deleted;
			}

			if (deleted >= batchSize) {
				// retrieve and delete another batch
				deleteCollection(collectionName, batchSize);
			}

		} catch (Exception e) {
			throw new FireStoreCustomException("Error in deleting collection" + e.getCause());
		}
	}

	/**
	 * Deletes the fields in document in a collection
	 *
	 * @param collectionName
	 * @param documentId
	 * @param fields
	 */
	@Override
	public void deleteFields(String collectionName, String documentId, List<String> fields) {
		DocumentReference docRef = this.fireStoreDb.collection(collectionName).document(documentId);
		Map<String, Object> updates = new HashMap<>();
		fields.forEach(field -> updates.put(field, FieldValue.delete()));
		docRef.update(updates);

	}

}
