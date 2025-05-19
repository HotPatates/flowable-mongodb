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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bson.conversions.Bson;
import org.flowable.common.engine.impl.persistence.cache.CachedEntityMatcher;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.engine.history.HistoricProcessInstance;
import org.flowable.engine.impl.ActivityInstanceQueryImpl;
import org.flowable.engine.impl.HistoricProcessInstanceQueryImpl;
import org.flowable.engine.impl.persistence.entity.ActivityInstanceEntity;
import org.flowable.engine.impl.persistence.entity.ActivityInstanceEntityImpl;
import org.flowable.engine.impl.persistence.entity.ExecutionEntity;
import org.flowable.engine.impl.persistence.entity.HistoricProcessInstanceEntity;
import org.flowable.engine.impl.persistence.entity.HistoricProcessInstanceEntityImpl;
import org.flowable.engine.impl.persistence.entity.data.ActivityInstanceDataManager;
import org.flowable.engine.impl.persistence.entity.data.HistoricProcessInstanceDataManager;
import org.flowable.engine.impl.persistence.entity.data.impl.cachematcher.ActivityInstanceMatcher;
import org.flowable.engine.runtime.ActivityInstance;
import org.flowable.mongodb.cfg.MongoDbProcessEngineConfiguration;
import org.flowable.task.service.impl.persistence.entity.TaskEntity;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;

public class MongoDbActivityInstanceDataManager extends AbstractMongoDbDataManager<ActivityInstanceEntity> implements ActivityInstanceDataManager {

    public static final String COLLECTION_ACTIVITY_INSTANCES = "activityInstances";

    protected CachedEntityMatcher<ActivityInstanceEntity> activityInstanceMatcher = new ActivityInstanceMatcher();

    public MongoDbActivityInstanceDataManager(MongoDbProcessEngineConfiguration processEngineConfiguration) {
        super(processEngineConfiguration);
    }

    @Override
    public String getCollection() {
        return COLLECTION_ACTIVITY_INSTANCES;
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        ActivityInstanceEntity activityInstanceEntity = (ActivityInstanceEntity) entity;
        BasicDBObject updateObject = null;
        updateObject = setUpdateProperty(activityInstanceEntity, "endTime", activityInstanceEntity.getEndTime(), updateObject);
        updateObject = setUpdateProperty(activityInstanceEntity, "transactionOrder", activityInstanceEntity.getTransactionOrder(), updateObject);
        updateObject = setUpdateProperty(activityInstanceEntity, "durationInMillis", activityInstanceEntity.getDurationInMillis(), updateObject);
        updateObject = setUpdateProperty(activityInstanceEntity, "deleteReason", activityInstanceEntity.getDeleteReason(), updateObject);
        updateObject = setUpdateProperty(activityInstanceEntity, "executionId", activityInstanceEntity.getExecutionId(), updateObject);
        updateObject = setUpdateProperty(activityInstanceEntity, "taskId", activityInstanceEntity.getTaskId(), updateObject);
        updateObject = setUpdateProperty(activityInstanceEntity, "assignee", activityInstanceEntity.getAssignee(), updateObject);
        updateObject = setUpdateProperty(activityInstanceEntity, "calledProcessInstanceId", activityInstanceEntity.getCalledProcessInstanceId(), updateObject);
        updateObject = setUpdateProperty(activityInstanceEntity, "activityId", activityInstanceEntity.getActivityId(), updateObject);
        updateObject = setUpdateProperty(activityInstanceEntity, "activityName", activityInstanceEntity.getActivityName(), updateObject);
        return updateObject;
    }

    @Override
    public List<ActivityInstanceEntity> findUnfinishedActivityInstancesByExecutionAndActivityId(String executionId, String activityId) {
        Bson filter = Filters.and(Filters.eq("executionId", executionId), Filters.eq("activityId", activityId));

        Map<String, Object> params = new HashMap<>();
        params.put("executionId", executionId);
        params.put("activityId", activityId);

        return getMongoDbSession().find(COLLECTION_ACTIVITY_INSTANCES, filter, params, ActivityInstanceEntityImpl.class, activityInstanceMatcher, true);
    }

    @Override
    public List<ActivityInstanceEntity> findActivityInstancesByExecutionIdAndActivityId(String executionId, String activityId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ActivityInstanceEntity findActivityInstanceByTaskId(String taskId) {
        Bson filter = Filters.eq("taskId", taskId);
        return getMongoDbSession().findOne(COLLECTION_ACTIVITY_INSTANCES, filter);
    }

    @Override
    public List<ActivityInstanceEntity> findActivityInstancesByProcessInstanceId(String processInstanceId, boolean includeDeleted) {
        Bson filter = Filters.eq("processInstanceId", processInstanceId);
        return getMongoDbSession().find(COLLECTION_ACTIVITY_INSTANCES, filter);
    }

    @Override
    public void deleteActivityInstancesByProcessInstanceId(String processInstanceId) {
        // TODO: add support for bulkDelete operation (cfr relational logic)
        List<ActivityInstanceEntity> activityInstancesByProcessInstanceId = findActivityInstancesByProcessInstanceId(processInstanceId, false);
        for (ActivityInstanceEntity activityInstanceEntity : activityInstancesByProcessInstanceId) {
            getMongoDbSession().delete(COLLECTION_ACTIVITY_INSTANCES, activityInstanceEntity);
        }
    }

    @Override
    public long findActivityInstanceCountByQueryCriteria(ActivityInstanceQueryImpl activityInstanceQuery) {
        return getMongoDbSession().count(COLLECTION_ACTIVITY_INSTANCES,createFilter(activityInstanceQuery));
    }

    @Override
    public List<ActivityInstance> findActivityInstancesByQueryCriteria(ActivityInstanceQueryImpl activityInstanceQuery) {
        return getMongoDbSession().find(COLLECTION_ACTIVITY_INSTANCES,createFilter(activityInstanceQuery));
    }

    @Override
    public List<ActivityInstance> findActivityInstancesByNativeQuery(Map<String, Object> parameterMap) {
        return getMongoDbSession().find(COLLECTION_ACTIVITY_INSTANCES,createFilter(parameterMap));
    }

    @Override
    public long findActivityInstanceCountByNativeQuery(Map<String, Object> parameterMap) {
        return getMongoDbSession().count(COLLECTION_ACTIVITY_INSTANCES,createFilter(parameterMap));
    }

    @Override
    public ActivityInstanceEntity create() {
        return new ActivityInstanceEntityImpl();
    }

    protected Bson createFilter(ActivityInstanceQueryImpl query) {
        List<Bson> filters = new ArrayList<>();
        if(query.getProcessInstanceId() != null) {
            filters.add(Filters.eq("processInstanceId", query.getProcessInstanceId()));
        }
        if(query.getProcessDefinitionId() != null) {
            filters.add(Filters.eq("processDefinitionId", query.getProcessDefinitionId()));
        }
        if(query.getDeleteReason() != null) {
            filters.add(Filters.eq("deleteReason", query.getDeleteReason()));
        }
        if(query.getActivityId() != null) {
            filters.add(Filters.eq("activityId", query.getActivityId()));
        }
        if(query.getActivityName() != null) {
            filters.add(Filters.eq("activityName", query.getActivityName()));
        }
        if(query.getActivityType() != null) {
            filters.add(Filters.eq("activityType", query.getActivityType()));
        }
        if(query.getExecutionId() != null) {
            filters.add(Filters.eq("executionId", query.getExecutionId()));
        }
        if(query.getAssignee() != null) {
            filters.add(Filters.eq("assignee", query.getAssignee()));
        }
        if(query.getTenantId() != null) {
            filters.add(Filters.eq("tenantId", query.getTenantId()));
        }
        return Filters.and(filters);
    }

    protected Bson createFilter(Map<String, Object> parameterMap) {
        List<Bson> filters = new ArrayList<>();
        if(parameterMap != null) {
            for (Map.Entry<String, Object> entry : parameterMap.entrySet()) {
                filters.add(Filters.eq(entry.getKey(), entry.getValue()));
            }
        }
        return Filters.and(filters);
    }
}
