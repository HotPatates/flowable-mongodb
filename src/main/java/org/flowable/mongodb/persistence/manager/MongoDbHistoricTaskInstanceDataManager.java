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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.bson.conversions.Bson;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.task.api.history.HistoricTaskInstance;
import org.flowable.task.service.impl.HistoricTaskInstanceQueryImpl;
import org.flowable.task.service.impl.persistence.entity.HistoricTaskInstanceEntity;
import org.flowable.task.service.impl.persistence.entity.HistoricTaskInstanceEntityImpl;
import org.flowable.task.service.impl.persistence.entity.TaskEntity;
import org.flowable.task.service.impl.persistence.entity.data.HistoricTaskInstanceDataManager;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;

public class MongoDbHistoricTaskInstanceDataManager extends AbstractMongoDbDataManager<HistoricTaskInstanceEntity> implements HistoricTaskInstanceDataManager {

    public static final String COLLECTION_HISTORIC_TASK_INSTANCES = "historicTaskInstances";

    @Override
    public String getCollection() {
        return COLLECTION_HISTORIC_TASK_INSTANCES;
    }

    @Override
    public HistoricTaskInstanceEntity create() {
        return new HistoricTaskInstanceEntityImpl();
    }

    @Override
    public HistoricTaskInstanceEntity create(TaskEntity task) {
        return new HistoricTaskInstanceEntityImpl(task);
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        HistoricTaskInstanceEntity historicTaskInstanceEntity = (HistoricTaskInstanceEntity) entity;
        BasicDBObject updateObject = null;
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "processDefinitionId", historicTaskInstanceEntity.getProcessDefinitionId(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "executionId", historicTaskInstanceEntity.getExecutionId(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "name", historicTaskInstanceEntity.getName(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "parentTaskId", historicTaskInstanceEntity.getParentTaskId(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "description", historicTaskInstanceEntity.getDescription(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "owner", historicTaskInstanceEntity.getOwner(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "assignee", historicTaskInstanceEntity.getAssignee(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "claimTime", historicTaskInstanceEntity.getClaimTime(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "endTime", historicTaskInstanceEntity.getEndTime(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "duration", historicTaskInstanceEntity.getDurationInMillis(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "deleteReason", historicTaskInstanceEntity.getDeleteReason(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "taskDefinitionKey", historicTaskInstanceEntity.getTaskDefinitionKey(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "formKey", historicTaskInstanceEntity.getFormKey(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "priority", historicTaskInstanceEntity.getPriority(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "dueDate", historicTaskInstanceEntity.getDueDate(), updateObject);
        updateObject = setUpdateProperty(historicTaskInstanceEntity, "category", historicTaskInstanceEntity.getCategory(), updateObject);
        return updateObject;
    }

    @Override
    public List<HistoricTaskInstanceEntity> findHistoricTasksByParentTaskId(String parentTaskId) {
        return getMongoDbSession().find(COLLECTION_HISTORIC_TASK_INSTANCES, Filters.eq("parentTaskId", parentTaskId));
    }

    @Override
    public List<String> findHistoricTaskIdsByParentTaskIds(Collection<String> parentTaskIds) {
        return getMongoDbSession().find(COLLECTION_HISTORIC_TASK_INSTANCES, Filters.in("parentTaskId", parentTaskIds));
    }

    @Override
    public List<HistoricTaskInstanceEntity> findHistoricTasksByProcessInstanceId(String processInstanceId) {
        return getMongoDbSession().find(COLLECTION_HISTORIC_TASK_INSTANCES, Filters.eq("processInstanceId", processInstanceId));
    }

    @Override
    public List<String> findHistoricTaskIdsForProcessInstanceIds(Collection<String> processInstanceIds) {
        return getMongoDbSession().find(COLLECTION_HISTORIC_TASK_INSTANCES, Filters.in("processInstanceId", processInstanceIds));
    }

    @Override
    public List<String> findHistoricTaskIdsForScopeIdsAndScopeType(Collection<String> scopeIds, String scopeType) {
        return getMongoDbSession().find(COLLECTION_HISTORIC_TASK_INSTANCES, Filters.and(
                Filters.eq("scopeId", scopeIds),
                       Filters.eq("scopeType", scopeType)
                )
        );
    }

    @Override
    public long findHistoricTaskInstanceCountByQueryCriteria(HistoricTaskInstanceQueryImpl query) {
        Bson filter = createFilter(query);
        return getMongoDbSession().count(COLLECTION_HISTORIC_TASK_INSTANCES, filter);
    }

    @Override
    public List<HistoricTaskInstance> findHistoricTaskInstancesByQueryCriteria(HistoricTaskInstanceQueryImpl query) {
        return getMongoDbSession().find(COLLECTION_HISTORIC_TASK_INSTANCES, createFilter(query));
    }

    @Override
    public List<HistoricTaskInstance> findHistoricTaskInstancesAndRelatedEntitiesByQueryCriteria(HistoricTaskInstanceQueryImpl query) {
        // MongoDB does not support joins natively, so we just return the base entity
        return findHistoricTaskInstancesByQueryCriteria(query);
    }

    @Override
    public List<HistoricTaskInstance> findHistoricTaskInstancesByNativeQuery(Map<String, Object> parameterMap) {
        BasicDBObject query = new BasicDBObject(parameterMap);
        return getMongoDbSession().find(COLLECTION_HISTORIC_TASK_INSTANCES, query);
    }

    @Override
    public long findHistoricTaskInstanceCountByNativeQuery(Map<String, Object> parameterMap) {
        BasicDBObject query = new BasicDBObject(parameterMap);
        return getMongoDbSession().count(COLLECTION_HISTORIC_TASK_INSTANCES, query);
    }

    @Override
    public void deleteHistoricTaskInstances(HistoricTaskInstanceQueryImpl query) {
        getMongoDbSession().find(COLLECTION_HISTORIC_TASK_INSTANCES,createFilter(query)).forEach(taskInstance -> {
            getMongoDbSession().delete(COLLECTION_HISTORIC_TASK_INSTANCES,(HistoricTaskInstanceEntity) taskInstance);
        });
    }

    @Override
    public void bulkDeleteHistoricTaskInstancesForIds(Collection<String> taskIds) {
        getMongoDbSession().bulkDelete(COLLECTION_HISTORIC_TASK_INSTANCES, Filters.in("taskId", taskIds));
    }

    @Override
    public void deleteHistoricTaskInstancesForNonExistingProcessInstances() {
    }

    @Override
    public void deleteHistoricTaskInstancesForNonExistingCaseInstances() {
    }

    protected Bson createFilter(HistoricTaskInstanceQueryImpl query) {
        List<Bson> filters = new ArrayList<>();

        if (query.getProcessInstanceId() != null) {
            filters.add(Filters.eq("processInstanceId", query.getProcessInstanceId()));
        }
        if (query.getTaskAssignee() != null) {
            filters.add(Filters.eq("assignee", query.getTaskAssignee()));
        }
        if (query.getExecutionId() != null) {
            filters.add(Filters.eq("executionId", query.getExecutionId()));
        }
        if (query.getTaskDefinitionKey() != null) {
            filters.add(Filters.eq("taskDefinitionKey", query.getTaskDefinitionKey()));
        }
        if (query.getCategory() != null) {
            filters.add(Filters.eq("category", query.getCategory()));
        }
        if (query.getTaskName() != null) {
            filters.add(Filters.eq("name", query.getTaskName()));
        }
        if (query.getTaskAssignee() != null) {
            filters.add(Filters.eq("assignee", query.getTaskAssignee()));
        }
        if (filters.isEmpty()) {
            return new BasicDBObject(); // no filtering
        } else {
            return Filters.and(filters);
        }
    }
}
