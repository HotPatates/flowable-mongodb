package org.flowable.mongodb.persistence.manager;

import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.engine.impl.persistence.entity.ProcessDefinitionInfoEntity;
import org.flowable.engine.impl.persistence.entity.data.ProcessDefinitionInfoDataManager;
import org.flowable.mongodb.cfg.MongoDbProcessEngineConfiguration;
import org.flowable.mongodb.persistence.entity.MongoProcessDefinitionInfoEntity;

import com.mongodb.BasicDBObject;
import org.bson.Document;

/**
 * @author Joram Barrez
 */
public class MongoDbProcessDefinitionInfoDataManager extends AbstractMongoDbDataManager<ProcessDefinitionInfoEntity>
        implements ProcessDefinitionInfoDataManager {

    public static final String COLLECTION_PROCESS_DEFINITION_INFO = "processDefinitionInfo";

    public MongoDbProcessDefinitionInfoDataManager(MongoDbProcessEngineConfiguration processEngineConfiguration) {
        super(processEngineConfiguration);
    }

    @Override
    public String getCollection() {
        return COLLECTION_PROCESS_DEFINITION_INFO;
    }

    @Override
    public ProcessDefinitionInfoEntity create() {
        return new MongoProcessDefinitionInfoEntity();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        MongoProcessDefinitionInfoEntity processDefinitionInfo = (MongoProcessDefinitionInfoEntity) entity;
        BasicDBObject updateObject = null;

        updateObject = setUpdateProperty(processDefinitionInfo, "processDefinitionId", processDefinitionInfo.getProcessDefinitionId(), updateObject);
        updateObject = setUpdateProperty(processDefinitionInfo, "infoJsonId", processDefinitionInfo.getInfoJsonId(), updateObject);
        updateObject = setUpdateProperty(processDefinitionInfo, "revision", processDefinitionInfo.getRevision(), updateObject);
        updateObject = setUpdateProperty(processDefinitionInfo, "latest", processDefinitionInfo.isLatest(), updateObject);

        return updateObject;
    }

    @Override
    public ProcessDefinitionInfoEntity findProcessDefinitionInfoByProcessDefinitionId(String processDefinitionId) {
        BasicDBObject query = new BasicDBObject("processDefinitionId", processDefinitionId);

        Document document = getMongoDbSession().getMongoDatabase()
                .getCollection(COLLECTION_PROCESS_DEFINITION_INFO)
                .find(query)
                .first();

        if (document != null) {
            MongoProcessDefinitionInfoEntity entity = new MongoProcessDefinitionInfoEntity();
            entity.setId(document.getString("id"));
            entity.setProcessDefinitionId(document.getString("processDefinitionId"));
            entity.setInfoJsonId(document.getString("infoJsonId"));
            entity.setRevision(document.getInteger("revision", 1));
            entity.setLatest(document.getBoolean("latest", false));
            return entity;
        }

        return null;
    }
}
