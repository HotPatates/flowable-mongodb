package org.flowable.mongodb.persistence.manager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.conversions.Bson;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.job.api.Job;
import org.flowable.job.service.impl.SuspendedJobQueryImpl;
import org.flowable.job.service.impl.persistence.entity.SuspendedJobEntity;
import org.flowable.job.service.impl.persistence.entity.SuspendedJobEntityImpl;
import org.flowable.job.service.impl.persistence.entity.data.SuspendedJobDataManager;
import org.flowable.mongodb.cfg.MongoDbJobServiceConfiguration;
import org.flowable.mongodb.cfg.MongoDbProcessEngineConfiguration;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;

public class MongoDbSuspendedJobDataManager extends AbstractMongoDbDataManager<SuspendedJobEntity> implements SuspendedJobDataManager {

    public static final String COLLECTION_SUSPENDED_JOBS = "suspendedJobs";

    public MongoDbSuspendedJobDataManager() {
        super();
    }

    @Override
    public String getCollection() {
        return COLLECTION_SUSPENDED_JOBS;
    }

    @Override
    public SuspendedJobEntity create() {
        return new SuspendedJobEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        SuspendedJobEntityImpl jobEntity = (SuspendedJobEntityImpl) entity;
        BasicDBObject updateObject = new BasicDBObject();
        updateObject.append("executionId", jobEntity.getExecutionId());
        updateObject.append("processInstanceId", jobEntity.getProcessInstanceId());
        updateObject.append("processDefinitionId", jobEntity.getProcessDefinitionId());
        updateObject.append("correlationId", jobEntity.getCorrelationId());
        updateObject.append("tenantId", jobEntity.getTenantId());
        return updateObject;
    }

    @Override
    public SuspendedJobEntity findJobByCorrelationId(String correlationId) {
        return getMongoDbSession().findOne(COLLECTION_SUSPENDED_JOBS, Filters.eq("correlationId", correlationId));
    }

    @Override
    public List<SuspendedJobEntity> findJobsByExecutionId(String executionId) {
        return getMongoDbSession().find(COLLECTION_SUSPENDED_JOBS, Filters.eq("executionId", executionId));
    }

    @Override
    public List<SuspendedJobEntity> findJobsByProcessInstanceId(String processInstanceId) {
        return getMongoDbSession().find(COLLECTION_SUSPENDED_JOBS, Filters.eq("processInstanceId", processInstanceId));
    }

    @Override
    public List<Job> findJobsByQueryCriteria(SuspendedJobQueryImpl jobQuery) {
        return new ArrayList<>(getMongoDbSession().find(COLLECTION_SUSPENDED_JOBS, createFilter(jobQuery)));
    }

    @Override
    public long findJobCountByQueryCriteria(SuspendedJobQueryImpl jobQuery) {
        return getMongoDbSession().count(COLLECTION_SUSPENDED_JOBS, createFilter(jobQuery));
    }

    protected Bson createFilter(SuspendedJobQueryImpl query) {
        List<Bson> filters = new ArrayList<>();

        if (query.getExecutionId() != null) {
            filters.add(Filters.eq("executionId", query.getExecutionId()));
        }
        if (query.getProcessInstanceId() != null) {
            filters.add(Filters.eq("processInstanceId", query.getProcessInstanceId()));
        }
        if (query.getProcessDefinitionId() != null) {
            filters.add(Filters.eq("processDefinitionId", query.getProcessDefinitionId()));
        }
        if (query.getTenantId() != null) {
            filters.add(Filters.eq("tenantId", query.getTenantId()));
        }
        if (query.isWithoutTenantId()) {
            filters.add(Filters.or(Filters.eq("tenantId", ""), Filters.exists("tenantId", false)));
        }
        // Add more filters as needed based on what's supported

        return makeAndFilter(filters);
    }

    @Override
    public void updateJobTenantIdForDeployment(String deploymentId, String newTenantId) {
        getMongoDbSession().bulkUpdate(COLLECTION_SUSPENDED_JOBS,
                Filters.eq("deploymentId", deploymentId),
                new BasicDBObject("$set", new BasicDBObject("tenantId", newTenantId)));
    }
}
