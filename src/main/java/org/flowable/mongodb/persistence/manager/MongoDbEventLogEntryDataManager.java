package org.flowable.mongodb.persistence.manager;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.engine.event.EventLogEntry;
import org.flowable.engine.impl.persistence.entity.EventLogEntryEntity;
import org.flowable.engine.impl.persistence.entity.EventLogEntryEntityImpl;
import org.flowable.engine.impl.persistence.entity.data.EventLogEntryDataManager;
import org.flowable.mongodb.cfg.MongoDbProcessEngineConfiguration;

import java.util.ArrayList;
import java.util.List;

public class MongoDbEventLogEntryDataManager extends AbstractMongoDbDataManager<EventLogEntryEntity> implements EventLogEntryDataManager {

    public static final String COLLECTION_EVENT_LOG_ENTRIES = "eventLogEntries";


    public MongoDbEventLogEntryDataManager(MongoDbProcessEngineConfiguration processEngineConfiguration) {
        super(processEngineConfiguration);
    }
    @Override
    public List<EventLogEntry> findAllEventLogEntries() {
        return getMongoDbSession().find(COLLECTION_EVENT_LOG_ENTRIES,new Document());
    }

    @Override
    public List<EventLogEntry> findEventLogEntries(long l, long l1) {
        List<Bson> filters = new ArrayList<>();
        filters.add(new Document("startLogNr", l));
        if (l1 > 0L) {
            filters.add(new Document("endLogNr", l + l1 + 1L));
        }
       return getMongoDbSession().find(COLLECTION_EVENT_LOG_ENTRIES, Filters.and(filters));
    }

    @Override
    public List<EventLogEntry> findEventLogEntriesByProcessInstanceId(String s) {
        return getMongoDbSession().find(COLLECTION_EVENT_LOG_ENTRIES, new Document("processInstanceId", s));
    }

    @Override
    public void deleteEventLogEntry(long logNr) {

    }

    @Override
    public String getCollection() {
        return COLLECTION_EVENT_LOG_ENTRIES;
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        EventLogEntryEntity instanceEntity = (EventLogEntryEntity) entity;
        BasicDBObject updateObject = null;
        updateObject = setUpdateProperty(instanceEntity, "type", instanceEntity.getType(), updateObject);
        updateObject = setUpdateProperty(instanceEntity, "taskId", instanceEntity.getTaskId(), updateObject);
        updateObject = setUpdateProperty(instanceEntity, "executionId", instanceEntity.getExecutionId(), updateObject);
        updateObject = setUpdateProperty(instanceEntity, "userId", instanceEntity.getUserId(), updateObject);
        updateObject = setUpdateProperty(instanceEntity, "processInstanceId", instanceEntity.getProcessInstanceId(), updateObject);
        updateObject = setUpdateProperty(instanceEntity, "processDefinitionId", instanceEntity.getProcessDefinitionId(), updateObject);
        updateObject = setUpdateProperty(instanceEntity, "lockOwner", instanceEntity.getLockOwner(), updateObject);
        updateObject = setUpdateProperty(instanceEntity, "processed", instanceEntity.getProcessed(), updateObject);
        return updateObject;
    }

    @Override
    public EventLogEntryEntity create() {
        return new EventLogEntryEntityImpl();
    }
}

