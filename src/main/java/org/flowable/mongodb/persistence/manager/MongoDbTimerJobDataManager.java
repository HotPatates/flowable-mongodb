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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.conversions.Bson;
import org.flowable.common.engine.impl.Page;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.engine.ProcessEngineConfiguration;
import org.flowable.job.api.Job;
import org.flowable.job.service.JobServiceConfiguration;
import org.flowable.job.service.impl.TimerJobQueryImpl;
import org.flowable.job.service.impl.persistence.entity.TimerJobEntity;
import org.flowable.job.service.impl.persistence.entity.TimerJobEntityImpl;
import org.flowable.job.service.impl.persistence.entity.data.TimerJobDataManager;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;

public class MongoDbTimerJobDataManager extends AbstractMongoDbDataManager<TimerJobEntity> implements TimerJobDataManager {

    public static final String COLLECTION_TIMER_JOBS = "timerJobs";

    protected JobServiceConfiguration jobServiceConfiguration;

    public MongoDbTimerJobDataManager(JobServiceConfiguration jobServiceConfiguration) {
        this.jobServiceConfiguration = jobServiceConfiguration;
    }

    @Override
    public String getCollection() {
        return COLLECTION_TIMER_JOBS;
    }

    @Override
    public TimerJobEntity create() {
        return new TimerJobEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        TimerJobEntity jobEntity = (TimerJobEntity) entity;
        BasicDBObject updateObject = new BasicDBObject();

        updateObject.append("retries", jobEntity.getRetries());
        if (jobEntity.getExceptionMessage() != null) {
            updateObject.append("exceptionMessage", jobEntity.getExceptionMessage());
        }
        if (jobEntity.getLockOwner() != null) {
            updateObject.append("lockOwner", jobEntity.getLockOwner());
        }
        if (jobEntity.getLockExpirationTime() != null) {
            updateObject.append("lockExpirationTime", jobEntity.getLockExpirationTime());
        }
        if (jobEntity.getDuedate() != null) {
            updateObject.append("duedate", jobEntity.getDuedate());
        }

        return updateObject;
    }

    @Override
    public TimerJobEntity findJobByCorrelationId(String correlationId) {
        return getMongoDbSession().findOne(COLLECTION_TIMER_JOBS, Filters.eq("correlationId", correlationId));
    }

    @Override
    public List<TimerJobEntity> findJobsByTypeAndProcessDefinitionId(String jobHandlerType, String processDefinitionId) {
        Bson filter = Filters.and(
                Filters.eq("jobHandlerType", jobHandlerType),
                Filters.eq("processDefinitionId", processDefinitionId)
        );
        return getMongoDbSession().find(COLLECTION_TIMER_JOBS, filter);
    }

    @Override
    public List<TimerJobEntity> findJobsByTypeAndProcessDefinitionKeyNoTenantId(String jobHandlerType, String processDefinitionKey) {
        Bson filter = Filters.and(
                Filters.eq("jobHandlerType", jobHandlerType),
                Filters.eq("processDefinitionKey", processDefinitionKey),
                Filters.or(
                        Filters.eq("tenantId", ProcessEngineConfiguration.NO_TENANT_ID),
                        Filters.not(Filters.exists("tenantId"))
                )
        );
        return getMongoDbSession().find(COLLECTION_TIMER_JOBS, filter);
    }

    @Override
    public List<TimerJobEntity> findJobsByTypeAndProcessDefinitionKeyAndTenantId(String jobHandlerType, String processDefinitionKey, String tenantId) {
        Bson filter = Filters.and(
                Filters.eq("jobHandlerType", jobHandlerType),
                Filters.eq("processDefinitionKey", processDefinitionKey),
                Filters.eq("tenantId", tenantId)
        );
        return getMongoDbSession().find(COLLECTION_TIMER_JOBS, filter);
    }

    @Override
    public List<TimerJobEntity> findJobsByScopeIdAndSubScopeId(String scopeId, String subScopeId) {
        Bson filter = Filters.and(
                Filters.eq("scopeId", scopeId),
                Filters.eq("subScopeId", subScopeId)
        );
        return getMongoDbSession().find(COLLECTION_TIMER_JOBS, filter);
    }

    @Override
    public List<TimerJobEntity> findJobsToExecute(List<String> enabledCategories, Page page) {
        List<Bson> filterParts = new ArrayList<>();

        if (jobServiceConfiguration.getJobExecutionScope() == null) {
            filterParts.add(Filters.eq("scopeType", null));
        } else if (!"all".equals(jobServiceConfiguration.getJobExecutionScope())) {
            filterParts.add(Filters.eq("scopeType", jobServiceConfiguration.getJobExecutionScope()));
        }

        filterParts.add(Filters.lte("duedate", jobServiceConfiguration.getClock().getCurrentTime()));
        filterParts.add(Filters.eq("lockOwner", null));

        Bson filter = Filters.and(filterParts);

        int skip = page != null ? page.getFirstResult() : 0;
        int limit = page != null ? page.getMaxResults() : 100;

        return getMongoDbSession().find(COLLECTION_TIMER_JOBS, filter, null, limit);
    }

    @Override
    public List<TimerJobEntity> findJobsByExecutionId(String executionId) {
        return getMongoDbSession().find(COLLECTION_TIMER_JOBS, Filters.eq("executionId", executionId));
    }

    @Override
    public List<TimerJobEntity> findJobsByProcessInstanceId(String processInstanceId) {
        return getMongoDbSession().find(COLLECTION_TIMER_JOBS, Filters.eq("processInstanceId", processInstanceId));
    }

    @Override
    public List<TimerJobEntity> findExpiredJobs(List<String> enabledCategories, Page page) {
        Bson filter = Filters.lte("duedate", jobServiceConfiguration.getClock().getCurrentTime());

        int skip = page != null ? page.getFirstResult() : 0;
        int limit = page != null ? page.getMaxResults() : 100;

        return getMongoDbSession().find(COLLECTION_TIMER_JOBS, filter, null, limit);
    }

    @Override
    public List<Job> findJobsByQueryCriteria(TimerJobQueryImpl timerJobQuery) {
        return new ArrayList<>(getMongoDbSession().find(COLLECTION_TIMER_JOBS, createFilter(timerJobQuery)));
    }

    @Override
    public long findJobCountByQueryCriteria(TimerJobQueryImpl timerJobQuery) {
        return getMongoDbSession().count(COLLECTION_TIMER_JOBS, createFilter(timerJobQuery));
    }

    protected Bson createFilter(TimerJobQueryImpl query) {
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
        if (query.getExecutable()) filters.add(Filters.lt("duedate", jobServiceConfiguration.getClock().getCurrentTime()));
        if (query.getDuedateHigherThan() != null) filters.add(Filters.gt("duedate", query.getDuedateHigherThan()));
        if (query.getDuedateLowerThan() != null) filters.add(Filters.lt("duedate", query.getDuedateLowerThan()));
        if (query.getDuedateHigherThanOrEqual() != null) filters.add(Filters.gte("duedate", query.getDuedateHigherThanOrEqual()));
        if (query.getDuedateLowerThanOrEqual() != null) filters.add(Filters.lte("duedate", query.getDuedateLowerThanOrEqual()));
        if (query.getExceptionMessage() != null) filters.add(Filters.eq("exceptionMessage", query.getExceptionMessage()));
        if (query.getTenantId() != null) filters.add(Filters.eq("tenantId", query.getTenantId()));
        if (query.getTenantIdLike() != null) filters.add(Filters.regex("tenantId", query.getTenantIdLike().replace("%", ".*")));
        if (query.isWithoutTenantId()) {
            filters.add(Filters.or(
                    Filters.eq("tenantId", ProcessEngineConfiguration.NO_TENANT_ID),
                    Filters.not(Filters.exists("tenantId"))
            ));
        }

        return filters.isEmpty() ? new BasicDBObject() : Filters.and(filters);
    }

    @Override
    public void updateJobTenantIdForDeployment(String deploymentId, String newTenantId) {
        Bson filter = Filters.eq("deploymentId", deploymentId);
        Bson update = Updates.set("tenantId", newTenantId);
        getMongoDbSession().bulkUpdate(COLLECTION_TIMER_JOBS, filter, update);
    }

    @Override
    public void resetExpiredJob(String jobId) {
        Bson filter = Filters.eq("_id", jobId);
        Bson update = Updates.combine(
                Updates.set("lockOwner", null),
                Updates.set("lockExpirationTime", null)
        );
        getMongoDbSession().updateOne(COLLECTION_TIMER_JOBS, filter, update);
    }
}
