package org.flowable.mongodb.persistence.manager;

import java.util.List;

import org.flowable.common.engine.impl.persistence.entity.ByteArrayEntity;
import org.flowable.common.engine.impl.persistence.entity.ByteArrayEntityImpl;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.common.engine.impl.persistence.entity.data.ByteArrayDataManager;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;

/**
 * MongoDB implementation of ByteArrayDataManager for jobs.
 * Stores binary data associated with jobs (e.g., serialized objects or attachments).
 *
 * Note: This class uses a separate collection ("jobByteArrays") than the general byte array storage.
 *
 * @author Joram Barrez
 */
public class MongoDbJobByteArrayDataManager extends AbstractMongoDbDataManager<ByteArrayEntity> implements ByteArrayDataManager {

    public static final String COLLECTION_JOB_BYTE_ARRAY = "jobByteArrays";

    @Override
    public String getCollection() {
        return COLLECTION_JOB_BYTE_ARRAY;
    }

    @Override
    public ByteArrayEntity create() {
        return new ByteArrayEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        ByteArrayEntity byteArray = (ByteArrayEntity) entity;
        BasicDBObject update = new BasicDBObject();
        update.append("name", byteArray.getName());
        update.append("bytes", byteArray.getBytes());
        update.append("deploymentId", byteArray.getDeploymentId());
        return update;
    }

    @Override
    public List<ByteArrayEntity> findAll() {
        return getMongoDbSession().find(COLLECTION_JOB_BYTE_ARRAY,null);
    }

    @Override
    public void deleteByteArrayNoRevisionCheck(String byteArrayEntityId) {
        getMongoDbSession().bulkDelete(COLLECTION_JOB_BYTE_ARRAY, Filters.eq("_id", byteArrayEntityId));
    }

}
