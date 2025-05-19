package org.flowable.mongodb.persistence.manager;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.job.api.Job;
import org.flowable.job.service.impl.DeadLetterJobQueryImpl;
import org.flowable.job.service.impl.persistence.entity.DeadLetterJobEntity;
import org.flowable.job.service.impl.persistence.entity.DeadLetterJobEntityImpl;
import org.flowable.job.service.impl.persistence.entity.data.DeadLetterJobDataManager;

import com.mongodb.BasicDBObject;

public class MongoDbDeadLetterJobDataManager extends AbstractMongoDbDataManager<DeadLetterJobEntity> implements DeadLetterJobDataManager {

    public static final String COLLECTION_DEADLETTER_JOBS = "deadLetterJobs";

    @Override
    public String getCollection() {
        return COLLECTION_DEADLETTER_JOBS;
    }

    @Override
    public DeadLetterJobEntity create() {
        return new DeadLetterJobEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        DeadLetterJobEntityImpl job = (DeadLetterJobEntityImpl) entity;
        BasicDBObject updateObject = null;
        updateObject = setUpdateProperty(job, "executionId", job.getExecutionId(), updateObject);
        updateObject = setUpdateProperty(job, "processInstanceId", job.getProcessInstanceId(), updateObject);
        updateObject = setUpdateProperty(job, "processDefinitionId", job.getProcessDefinitionId(), updateObject);
        updateObject = setUpdateProperty(job, "scopeId", job.getScopeId(), updateObject);
        updateObject = setUpdateProperty(job, "subScopeId", job.getSubScopeId(), updateObject);
        updateObject = setUpdateProperty(job, "scopeType", job.getScopeType(), updateObject);
        updateObject = setUpdateProperty(job, "elementId", job.getElementId(), updateObject);
        updateObject = setUpdateProperty(job, "elementName", job.getElementName(), updateObject);
        updateObject = setUpdateProperty(job, "exceptionMessage", job.getExceptionMessage(), updateObject);
        updateObject = setUpdateProperty(job, "exceptionStacktrace", job.getExceptionStacktrace(), updateObject);
        updateObject = setUpdateProperty(job, "retries", job.getRetries(), updateObject);
        updateObject = setUpdateProperty(job, "duedate", job.getDuedate(), updateObject);
        updateObject = setUpdateProperty(job, "tenantId", job.getTenantId(), updateObject);
        updateObject = setUpdateProperty(job, "correlationId", job.getCorrelationId(), updateObject);
        return updateObject;
    }

    protected DeadLetterJobEntityImpl mapToEntity(Document doc) {
        DeadLetterJobEntityImpl entity = new DeadLetterJobEntityImpl();
        entity.setId(doc.getString("id"));
        entity.setExecutionId(doc.getString("executionId"));
        entity.setProcessInstanceId(doc.getString("processInstanceId"));
        entity.setProcessDefinitionId(doc.getString("processDefinitionId"));
        entity.setScopeId(doc.getString("scopeId"));
        entity.setSubScopeId(doc.getString("subScopeId"));
        entity.setScopeType(doc.getString("scopeType"));
        entity.setElementId(doc.getString("elementId"));
        entity.setElementName(doc.getString("elementName"));
        entity.setExceptionMessage(doc.getString("exceptionMessage"));
        entity.setExceptionStacktrace(doc.getString("exceptionStacktrace"));
        entity.setRetries(doc.getInteger("retries"));
        entity.setDuedate(doc.getDate("duedate"));
        entity.setTenantId(doc.getString("tenantId"));
        entity.setCorrelationId(doc.getString("correlationId"));
        return entity;
    }

    @Override
    public DeadLetterJobEntity findJobByCorrelationId(String correlationId) {
        List<Document> result = getMongoDbSession().find(COLLECTION_DEADLETTER_JOBS, new Document("correlationId", correlationId));
        if (result.isEmpty()) {
            return null;
        }
        return mapToEntity(result.get(0));
    }

    @Override
    public List<DeadLetterJobEntity> findJobsByExecutionId(String executionId) {
        List<Document> results = getMongoDbSession().find(COLLECTION_DEADLETTER_JOBS, new Document("executionId", executionId));
        List<DeadLetterJobEntity> jobs = new ArrayList<>();
        for (Document doc : results) {
            jobs.add(mapToEntity(doc));
        }
        return jobs;
    }

    @Override
    public List<DeadLetterJobEntity> findJobsByProcessInstanceId(String processInstanceId) {
        List<Document> results = getMongoDbSession().find(COLLECTION_DEADLETTER_JOBS, new Document("processInstanceId", processInstanceId));
        List<DeadLetterJobEntity> jobs = new ArrayList<>();
        for (Document doc : results) {
            jobs.add(mapToEntity(doc));
        }
        return jobs;
    }

    @Override
    public List<Job> findJobsByQueryCriteria(DeadLetterJobQueryImpl jobQuery) {
        BasicDBObject query = new BasicDBObject();

        if (jobQuery.getExecutionId() != null) {
            query.append("executionId", jobQuery.getExecutionId());
        }
        if (jobQuery.getProcessInstanceId() != null) {
            query.append("processInstanceId", jobQuery.getProcessInstanceId());
        }
        if (jobQuery.getScopeId() != null) {
            query.append("scopeId", jobQuery.getScopeId());
        }
        if (jobQuery.getSubScopeId() != null) {
            query.append("subScopeId", jobQuery.getSubScopeId());
        }
        if (jobQuery.getScopeType() != null) {
            query.append("scopeType", jobQuery.getScopeType());
        }
        if (jobQuery.getElementId() != null) {
            query.append("elementId", jobQuery.getElementId());
        }
        if (jobQuery.getTenantId() != null) {
            query.append("tenantId", jobQuery.getTenantId());
        }

        List<Document> results = getMongoDbSession().find(COLLECTION_DEADLETTER_JOBS, query);
        List<Job> jobs = new ArrayList<>();
        for (Document doc : results) {
            jobs.add(mapToEntity(doc));
        }

        return jobs;
    }

    @Override
    public long findJobCountByQueryCriteria(DeadLetterJobQueryImpl jobQuery) {
        BasicDBObject query = new BasicDBObject();

        if (jobQuery.getExecutionId() != null) {
            query.append("executionId", jobQuery.getExecutionId());
        }
        if (jobQuery.getProcessInstanceId() != null) {
            query.append("processInstanceId", jobQuery.getProcessInstanceId());
        }
        if (jobQuery.getScopeId() != null) {
            query.append("scopeId", jobQuery.getScopeId());
        }
        if (jobQuery.getSubScopeId() != null) {
            query.append("subScopeId", jobQuery.getSubScopeId());
        }
        if (jobQuery.getScopeType() != null) {
            query.append("scopeType", jobQuery.getScopeType());
        }
        if (jobQuery.getElementId() != null) {
            query.append("elementId", jobQuery.getElementId());
        }
        if (jobQuery.getTenantId() != null) {
            query.append("tenantId", jobQuery.getTenantId());
        }

        return getMongoDbSession().count(COLLECTION_DEADLETTER_JOBS, query);
    }

    @Override
    public void updateJobTenantIdForDeployment(String deploymentId, String newTenantId) {
        BasicDBObject query = new BasicDBObject("deploymentId", deploymentId);
        List<DeadLetterJobEntity> jobs = getMongoDbSession().find(COLLECTION_DEADLETTER_JOBS, query);
        if (jobs != null) {
            for (DeadLetterJobEntity job : jobs) {
                job.setTenantId(newTenantId);
                getMongoDbSession().update(job);
            }
        }
    }



}
