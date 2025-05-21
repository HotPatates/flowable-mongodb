package org.flowable.mongodb.persistence.manager;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.conversions.Bson;
import org.flowable.common.engine.impl.Page;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.engine.ProcessEngineConfiguration;
import org.flowable.job.api.Job;
import org.flowable.job.api.JobInfo;
import org.flowable.job.service.JobServiceConfiguration;
import org.flowable.job.service.impl.JobQueryImpl;
import org.flowable.job.service.impl.persistence.entity.JobEntity;
import org.flowable.job.service.impl.persistence.entity.JobEntityImpl;
import org.flowable.job.service.impl.persistence.entity.data.JobDataManager;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

public class MongoDbJobDataManager extends AbstractMongoDbDataManager<JobEntity> implements JobDataManager {

    public static final String COLLECTION_JOBS = "jobs";

    protected JobServiceConfiguration jobServiceConfiguration;

    public MongoDbJobDataManager(JobServiceConfiguration jobServiceConfiguration) {
        this.jobServiceConfiguration = jobServiceConfiguration;
    }

    @Override
    public String getCollection() {
        return COLLECTION_JOBS;
    }

    @Override
    public JobEntity create() {
        return new JobEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        JobEntity jobEntity = (JobEntity) entity;
        BasicDBObject updateObject = new BasicDBObject();

        updateObject.put("retries", jobEntity.getRetries());

        if (jobEntity.getExceptionMessage() != null) {
            updateObject.put("exceptionMessage", jobEntity.getExceptionMessage());
        }
        if (jobEntity.getLockOwner() != null) {
            updateObject.put("lockOwner", jobEntity.getLockOwner());
        }
        if (jobEntity.getLockExpirationTime() != null) {
            updateObject.put("lockExpirationTime", jobEntity.getLockExpirationTime());
        }
        if (jobEntity.getTenantId() != null) {
            updateObject.put("tenantId", jobEntity.getTenantId());
        }

        return updateObject;
    }

    @Override
    public List<JobEntity> findJobsToExecute(List<String> enabledCategories, Page page) {
        Bson filter;
        if (jobServiceConfiguration.getJobExecutionScope() == null) {
            filter = Filters.and(Filters.eq("scopeType", null), Filters.eq("lockExpirationTime", null));
        } else if (!"all".equals(jobServiceConfiguration.getJobExecutionScope())) {
            filter = Filters.and(Filters.eq("scopeType", jobServiceConfiguration.getJobExecutionScope()),
                    Filters.eq("lockExpirationTime", null));
        } else {
            filter = Filters.eq("lockExpirationTime", null);
        }
        return getMongoDbSession().find(COLLECTION_JOBS, filter, null, 1);
    }

    @Override
    public List<JobEntity> findJobsByExecutionId(String executionId) {
        Bson filter = Filters.eq("executionId", executionId);
        return getMongoDbSession().find(COLLECTION_JOBS, filter);
    }

    @Override
    public List<JobEntity> findJobsByProcessInstanceId(String processInstanceId) {
        Bson filter = Filters.eq("processInstanceId", processInstanceId);
        return getMongoDbSession().find(COLLECTION_JOBS, filter);
    }

    @Override
    public List<JobEntity> findExpiredJobs(List<String> enabledCategories, Page page) {
        Bson filter;
        if (jobServiceConfiguration.getJobExecutionScope() == null) {
            filter = Filters.eq("scopeType", null);
        } else if (!"all".equals(jobServiceConfiguration.getJobExecutionScope())) {
            filter = Filters.eq("scopeType", jobServiceConfiguration.getJobExecutionScope());
        } else {
            filter = null;
        }

        Date now = jobServiceConfiguration.getClock().getCurrentTime();
        Date maxTimeout = new Date(now.getTime() - jobServiceConfiguration.getAsyncExecutorResetExpiredJobsMaxTimeout());

        Bson expirationFilter = Filters.or(
                Filters.lt("lockExpirationTime", now),
                Filters.and(Filters.eq("lockExpirationTime", null), Filters.lt("createTime", maxTimeout))
        );

        Bson finalFilter = (filter != null) ? Filters.and(filter, expirationFilter) : expirationFilter;

        return getMongoDbSession().find(COLLECTION_JOBS, finalFilter, null, 1);
    }

    @Override
    public void resetExpiredJob(String jobId) {
        Bson filter = Filters.eq("_id", jobId);
        Bson update = Updates.combine(
                Updates.unset("lockOwner"),
                Updates.unset("lockExpirationTime")
        );
        getMongoDbSession().updateOne(COLLECTION_JOBS, filter, update);
    }

    @Override
    public void updateJobTenantIdForDeployment(String deploymentId, String newTenantId) {
        Bson filter = Filters.eq("deploymentId", deploymentId);
        Bson update = Updates.set("tenantId", newTenantId);
        getMongoDbSession().bulkUpdate(COLLECTION_JOBS, filter, update);
    }

    @Override
    public void bulkUpdateJobLockWithoutRevisionCheck(List<JobEntity> jobEntities, String lockOwner, Date lockExpirationTime) {
        getMongoDbSession().bulkDelete(COLLECTION_JOBS,Filters.in("_id", jobEntities.stream().map(JobInfo::getId).toArray()));
    }

    @Override
    public JobEntity findJobByCorrelationId(String correlationId) {
        Bson filter = Filters.eq("correlationId", correlationId);
        return getMongoDbSession().findOne(COLLECTION_JOBS, filter);
    }

    @Override
    public List<Job> findJobsByQueryCriteria(JobQueryImpl jobQuery) {
        return getMongoDbSession().find(COLLECTION_JOBS, createFilter(jobQuery));
    }

    @Override
    public long findJobCountByQueryCriteria(JobQueryImpl jobQuery) {
        return getMongoDbSession().count(COLLECTION_JOBS, createFilter(jobQuery));
    }

    protected Bson createFilter(JobQueryImpl query) {
        List<Bson> filters = new ArrayList<>();

        if (query.getId() != null) filters.add(Filters.eq("_id", query.getId()));
        if (query.getExecutionId() != null) filters.add(Filters.eq("executionId", query.getExecutionId()));
        if (query.getProcessInstanceId() != null) filters.add(Filters.eq("processInstanceId", query.getProcessInstanceId()));
        if (query.getHandlerType() != null) filters.add(Filters.eq("jobHandlerType", query.getHandlerType()));
        if (query.getProcessDefinitionId() != null) filters.add(Filters.eq("processDefinitionId", query.getProcessDefinitionId()));
        if (query.getScopeId() != null) filters.add(Filters.eq("scopeId", query.getScopeId()));
        if (query.getSubScopeId() != null) filters.add(Filters.eq("subScopeId", query.getSubScopeId()));
        if (query.getScopeType() != null) filters.add(Filters.eq("scopeType", query.getScopeType()));
        if (query.getScopeDefinitionId() != null) filters.add(Filters.eq("scopeDefinitionId", query.getScopeDefinitionId()));
        if (query.getDuedateHigherThan() != null) filters.add(Filters.gt("duedate", query.getDuedateHigherThan()));
        if (query.getDuedateLowerThan() != null) filters.add(Filters.lt("duedate", query.getDuedateLowerThan()));
        if (query.getDuedateHigherThanOrEqual() != null) filters.add(Filters.gte("duedate", query.getDuedateHigherThanOrEqual()));
        if (query.getDuedateLowerThanOrEqual() != null) filters.add(Filters.lte("duedate", query.getDuedateLowerThanOrEqual()));

        if (query.isWithException()) {
            filters.add(Filters.exists("exceptionMessage", true));
        }
        if (query.getExceptionMessage() != null) filters.add(Filters.eq("exceptionMessage", query.getExceptionMessage()));
        if (query.getTenantId() != null) filters.add(Filters.eq("tenantId", query.getTenantId()));
        if (query.getTenantIdLike() != null) filters.add(Filters.regex("tenantId", query.getTenantIdLike().replace("%", ".*")));
        if (query.isWithoutTenantId()) {
            filters.add(Filters.or(
                    Filters.eq("tenantId", ProcessEngineConfiguration.NO_TENANT_ID),
                    Filters.not(Filters.exists("tenantId"))
            ));
        }

        return makeAndFilter(filters);
    }

    @Override
    public void deleteJobsByExecutionId(String executionId) {
        List<JobEntity> jobs = findJobsByExecutionId(executionId);
        for (JobEntity jobEntity : jobs) {
            delete(jobEntity);
        }
    }
}
