/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flowable.mongodb.persistence.manager;

import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.bson.conversions.Bson;
import org.flowable.common.engine.impl.Page;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.engine.impl.persistence.entity.ExecutionEntityImpl;
import org.flowable.job.api.ExternalWorkerJob;
import org.flowable.job.api.Job;
import org.flowable.job.service.impl.ExternalWorkerJobAcquireBuilderImpl;
import org.flowable.job.service.impl.ExternalWorkerJobQueryImpl;
import org.flowable.job.service.impl.SuspendedJobQueryImpl;
import org.flowable.job.service.impl.persistence.entity.ExternalWorkerJobEntity;
import org.flowable.job.service.impl.persistence.entity.ExternalWorkerJobEntityImpl;
import org.flowable.job.service.impl.persistence.entity.SuspendedJobEntity;
import org.flowable.job.service.impl.persistence.entity.data.ExternalWorkerJobDataManager;
import org.flowable.job.service.impl.persistence.entity.data.SuspendedJobDataManager;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;

/**
 * @author Joram Barrez
 */
public class MongoDbExternalWorkerJobDataManager extends AbstractMongoDbDataManager<ExternalWorkerJobEntity> implements ExternalWorkerJobDataManager {

    public static final String COLLECTION_EXTERNAL_WORKER_JOBS = "externalWorkerJobs";

    @Override
    public ExternalWorkerJobEntity findJobByCorrelationId(String correlationId) {
        Bson filter = Filters.eq("correlationId", correlationId);
        return getMongoDbSession().findOne(COLLECTION_EXTERNAL_WORKER_JOBS, filter);
    }

    @Override
    public List<ExternalWorkerJob> findJobsByQueryCriteria(ExternalWorkerJobQueryImpl jobQuery) {
        Bson filter = buildQueryFilter(jobQuery);
        return getMongoDbSession().find(COLLECTION_EXTERNAL_WORKER_JOBS, filter);
    }

    @Override
    public long findJobCountByQueryCriteria(ExternalWorkerJobQueryImpl jobQuery) {
        Bson filter = buildQueryFilter(jobQuery);
        return getMongoDbSession().count(COLLECTION_EXTERNAL_WORKER_JOBS, filter);
    }

    @Override
    public void deleteJobsByExecutionId(String executionId) {
        Bson filter = Filters.eq("executionId", executionId);
        List<ExternalWorkerJobEntity> jobs = getMongoDbSession().find(COLLECTION_EXTERNAL_WORKER_JOBS, filter);
        for (ExternalWorkerJobEntity job : jobs) {
            getMongoDbSession().delete(COLLECTION_EXTERNAL_WORKER_JOBS, job);
        }
    }

    @Override
    public List<ExternalWorkerJobEntity> findExternalJobsToExecute(ExternalWorkerJobAcquireBuilderImpl builder, int numberOfJobs) {
        Bson filter = Filters.and(
                Filters.eq("lockOwner", null),
                Filters.eq("suspended", false)
        );
        return getMongoDbSession().find(COLLECTION_EXTERNAL_WORKER_JOBS, filter,null,numberOfJobs);
    }

    @Override
    public List<ExternalWorkerJobEntity> findJobsByScopeIdAndSubScopeId(String scopeId, String subScopeId) {
        Bson filter = Filters.and(
                Filters.eq("scopeId", scopeId),
                Filters.eq("subScopeId", subScopeId)
        );
        return getMongoDbSession().find(COLLECTION_EXTERNAL_WORKER_JOBS, filter);
    }

    @Override
    public List<ExternalWorkerJobEntity> findJobsByWorkerId(String workerId) {
        return getMongoDbSession().find(COLLECTION_EXTERNAL_WORKER_JOBS, Filters.eq("workerId", workerId));
    }

    @Override
    public List<ExternalWorkerJobEntity> findJobsByWorkerIdAndTenantId(String workerId, String tenantId) {
        return getMongoDbSession().find(COLLECTION_EXTERNAL_WORKER_JOBS, Filters.and(Filters.eq("workerId", workerId), Filters.eq("tenantId", tenantId)));
    }

    @Override
    public List<ExternalWorkerJobEntity> findJobsToExecute(List<String> enabledCategories, Page page) {
        Bson filter = Filters.in("category", enabledCategories);
        return getMongoDbSession().find(COLLECTION_EXTERNAL_WORKER_JOBS, filter);
    }

    @Override
    public List<ExternalWorkerJobEntity> findJobsByExecutionId(String s) {
        Bson filter = Filters.eq("executionId", s);
        return getMongoDbSession().find(COLLECTION_EXTERNAL_WORKER_JOBS, filter);
    }

    @Override
    public List<ExternalWorkerJobEntity> findJobsByProcessInstanceId(String processInstanceId) {
        Bson filter = Filters.eq("processInstanceId", processInstanceId);
        return getMongoDbSession().find(COLLECTION_EXTERNAL_WORKER_JOBS, filter);
    }

    @Override
    public List<ExternalWorkerJobEntity> findExpiredJobs(List<String> enabledCategories, Page page) {
        Bson filter = Filters.and(
                Filters.in("category", enabledCategories),
                Filters.lt("expirationTime", new java.util.Date())
        );
        return getMongoDbSession().find(COLLECTION_EXTERNAL_WORKER_JOBS, filter);
    }

    @Override
    public void updateJobTenantIdForDeployment(String deploymentId, String newTenantId) {
        Bson filter = Filters.eq("deploymentId", deploymentId);
        List<ExternalWorkerJobEntity> jobs = getMongoDbSession().find(COLLECTION_EXTERNAL_WORKER_JOBS, filter);
        for (ExternalWorkerJobEntity job : jobs) {
            job.setTenantId(newTenantId);
            getMongoDbSession().update(job);
        }
    }

    @Override
    public void bulkUpdateJobLockWithoutRevisionCheck(List<ExternalWorkerJobEntity> jobEntities, String lockOwner, Date lockExpirationTime) {
        jobEntities.forEach(jobEntity -> {
            getMongoDbSession().update(jobEntity);
        });
    }

    @Override
    public void resetExpiredJob(String jobId) {
        Bson filter = Filters.eq("_id", jobId);
        Bson update =  new BasicDBObject("lockOwner", null)
                .append("lockExpirationTime", null)
                .append("retries", 0);
        getMongoDbSession().updateOne(COLLECTION_EXTERNAL_WORKER_JOBS, filter, update);
    }

    @Override
    public String getCollection() {
        return COLLECTION_EXTERNAL_WORKER_JOBS;
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        ExternalWorkerJobEntity jobEntity = (ExternalWorkerJobEntity) entity;
        BasicDBObject update = new BasicDBObject();

        update.append("revision", jobEntity.getRevision());


        if (jobEntity.getExecutionId() != null) {
            update.append("executionId", jobEntity.getExecutionId());
        }

        if (jobEntity.getProcessInstanceId() != null) {
            update.append("processInstanceId", jobEntity.getProcessInstanceId());
        }

        if (jobEntity.getProcessDefinitionId() != null) {
            update.append("processDefinitionId", jobEntity.getProcessDefinitionId());
        }

        if (jobEntity.getScopeId() != null) {
            update.append("scopeId", jobEntity.getScopeId());
        }

        if (jobEntity.getSubScopeId() != null) {
            update.append("subScopeId", jobEntity.getSubScopeId());
        }

        if (jobEntity.getScopeDefinitionId() != null) {
            update.append("scopeDefinitionId", jobEntity.getScopeDefinitionId());
        }

        if (jobEntity.getScopeType() != null) {
            update.append("scopeType", jobEntity.getScopeType());
        }

        if (jobEntity.getJobHandlerType() != null) {
            update.append("jobHandlerType", jobEntity.getJobHandlerType());
        }

        if (jobEntity.getJobHandlerConfiguration() != null) {
            update.append("jobHandlerConfiguration", jobEntity.getJobHandlerConfiguration());
        }

        update.append("retries", jobEntity.getRetries());

        if (jobEntity.getExceptionMessage() != null) {
            update.append("exceptionMessage", jobEntity.getExceptionMessage());
        }

        if (jobEntity.getExceptionStacktrace() != null) {
            update.append("exceptionStacktrace", jobEntity.getExceptionStacktrace());
        }

        if (jobEntity.getTenantId() != null) {
            update.append("tenantId", jobEntity.getTenantId());
        }

        if (jobEntity.getLockOwner() != null) {
            update.append("lockOwner", jobEntity.getLockOwner());
        }

        if (jobEntity.getLockExpirationTime() != null) {
            update.append("lockExpirationTime", jobEntity.getLockExpirationTime());
        }

        if (jobEntity.getCorrelationId() != null) {
            update.append("correlationId", jobEntity.getCorrelationId());
        }

        if (jobEntity.getCategory() != null) {
            update.append("category", jobEntity.getCategory());
        }

        return update;
    }


    @Override
    public ExternalWorkerJobEntity create() {
        return new ExternalWorkerJobEntityImpl();
    }
    protected Bson buildQueryFilter(ExternalWorkerJobQueryImpl jobQuery) {
        List<Bson> filters = new java.util.ArrayList<>();

        if (jobQuery.getId() != null) {
            filters.add(Filters.eq("_id", jobQuery.getId()));
        }

        if (jobQuery.getProcessInstanceId() != null) {
            filters.add(Filters.eq("processInstanceId", jobQuery.getProcessInstanceId()));
        }

        if (jobQuery.getExecutionId() != null) {
            filters.add(Filters.eq("executionId", jobQuery.getExecutionId()));
        }

        if (jobQuery.getTenantId() != null) {
            filters.add(Filters.eq("tenantId", jobQuery.getTenantId()));
        }

        if (jobQuery.getCategory() != null) {
            filters.add(Filters.eq("category", jobQuery.getCategory()));
        }

        if (jobQuery.getCorrelationId() != null) {
            filters.add(Filters.eq("correlationId", jobQuery.getCorrelationId()));
        }

        if (jobQuery.getScopeId() != null) {
            filters.add(Filters.eq("scopeId", jobQuery.getScopeId()));
        }

        if (jobQuery.getSubScopeId() != null) {
            filters.add(Filters.eq("subScopeId", jobQuery.getSubScopeId()));
        }

        if (jobQuery.getScopeType() != null) {
            filters.add(Filters.eq("scopeType", jobQuery.getScopeType()));
        }

        if (jobQuery.getLockOwner() != null) {
            filters.add(Filters.eq("lockOwner", jobQuery.getLockOwner()));
        }


        if (filters.isEmpty()) {
            return new BasicDBObject();
        }

        return Filters.and(filters);
    }


}
