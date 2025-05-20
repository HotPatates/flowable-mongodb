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
import java.util.List;
import java.util.Map;

import org.bson.conversions.Bson;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.task.api.Task;
import org.flowable.task.service.impl.TaskQueryImpl;
import org.flowable.task.service.impl.TaskQueryProperty;
import org.flowable.task.service.impl.persistence.entity.TaskEntity;
import org.flowable.task.service.impl.persistence.entity.TaskEntityImpl;
import org.flowable.task.service.impl.persistence.entity.data.TaskDataManager;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

public class MongoDbTaskDataManager extends AbstractMongoDbDataManager<TaskEntity> implements TaskDataManager {

    public static final String COLLECTION_TASKS = "tasks";

    @Override
    public String getCollection() {
        return COLLECTION_TASKS;
    }

    @Override
    public TaskEntity create() {
        return new TaskEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        TaskEntity taskEntity = (TaskEntity) entity;
        BasicDBObject updateObject = new BasicDBObject();
        if (taskEntity.getAssignee() != null) {
            updateObject.put("assignee", taskEntity.getAssignee());
        }
        if (taskEntity.getOwner() != null) {
            updateObject.put("owner", taskEntity.getOwner());
        }
        if (taskEntity.getName() != null) {
            updateObject.put("name", taskEntity.getName());
        }
        if (taskEntity.getTenantId() != null) {
            updateObject.put("tenantId", taskEntity.getTenantId());
        }

        return new BasicDBObject("$set", updateObject);
    }

    @Override
    public List<TaskEntity> findTasksByExecutionId(String executionId) {
        return getMongoDbSession().find(COLLECTION_TASKS, Filters.eq("executionId", executionId));
    }

    @Override
    public List<TaskEntity> findTasksByProcessInstanceId(String processInstanceId) {
        return getMongoDbSession().find(COLLECTION_TASKS, Filters.eq("processInstanceId", processInstanceId));
    }

    @Override
    public List<TaskEntity> findTasksByScopeIdAndScopeType(String scopeId, String scopeType) {
        Bson filter = Filters.and(
                Filters.eq("scopeId", scopeId),
                Filters.eq("scopeType", scopeType)
        );
        return getMongoDbSession().find(COLLECTION_TASKS, filter);
    }

    @Override
    public List<TaskEntity> findTasksBySubScopeIdAndScopeType(String subScopeId, String scopeType) {
        Bson filter = Filters.and(
                Filters.eq("subScopeId", subScopeId),
                Filters.eq("scopeType", scopeType)
        );
        return getMongoDbSession().find(COLLECTION_TASKS, filter);
    }

    @Override
    public List<Task> findTasksByQueryCriteria(TaskQueryImpl taskQuery) {
        return getMongoDbSession().find(COLLECTION_TASKS, createFilter(taskQuery), createSort(taskQuery));
    }

    @Override
    public long findTaskCountByQueryCriteria(TaskQueryImpl taskQuery) {
        Bson filter = createFilter(taskQuery);
        return getMongoDbSession().count(COLLECTION_TASKS, filter);
    }

    @Override
    public List<Task> findTasksWithRelatedEntitiesByQueryCriteria(TaskQueryImpl taskQuery) {
        // MongoDB doesn't support joins natively; return regular tasks
        return findTasksByQueryCriteria(taskQuery);
    }

    @Override
    public List<Task> findTasksByNativeQuery(Map<String, Object> parameterMap) {
        List<Bson> filters = new ArrayList<>();
        for (Map.Entry<String, Object> entry : parameterMap.entrySet()) {
            filters.add(Filters.eq(entry.getKey(), entry.getValue()));
        }
        Bson filter = filters.isEmpty() ? new BasicDBObject() : Filters.and(filters);
        return getMongoDbSession().find(COLLECTION_TASKS, filter);
    }

    @Override
    public long findTaskCountByNativeQuery(Map<String, Object> parameterMap) {
        List<Bson> filters = new ArrayList<>();
        for (Map.Entry<String, Object> entry : parameterMap.entrySet()) {
            filters.add(Filters.eq(entry.getKey(), entry.getValue()));
        }
        Bson filter = filters.isEmpty() ? new BasicDBObject() : Filters.and(filters);
        return getMongoDbSession().count(COLLECTION_TASKS, filter);
    }

    @Override
    public List<Task> findTasksByParentTaskId(String parentTaskId) {
        return getMongoDbSession().find(COLLECTION_TASKS, Filters.eq("parentTaskId", parentTaskId));
    }

    @Override
    public void updateTaskTenantIdForDeployment(String deploymentId, String newTenantId) {
        Bson filter = Filters.eq("deploymentId", deploymentId);
        BasicDBObject update = new BasicDBObject("$set", new BasicDBObject("tenantId", newTenantId));
        getMongoDbSession().bulkUpdate(COLLECTION_TASKS, filter, update);
    }

    @Override
    public void updateAllTaskRelatedEntityCountFlags(boolean newValue) {
        BasicDBObject update = new BasicDBObject("$set", new BasicDBObject("countEnabled", newValue));
        getMongoDbSession().bulkUpdate(COLLECTION_TASKS, new BasicDBObject(), update);
    }

    @Override
    public void deleteTasksByExecutionId(String executionId) {
        List<TaskEntity> tasksEntities = findTasksByExecutionId(executionId);
        for (TaskEntity taskEntity : tasksEntities) {
            getMongoDbSession().delete(COLLECTION_TASKS, taskEntity);
        }
    }

    protected Bson createFilter(TaskQueryImpl taskQuery) {
        List<Bson> filters = new ArrayList<>();

        if (taskQuery.getExecutionId() != null) {
            filters.add(Filters.eq("executionId", taskQuery.getExecutionId()));
        }
        if (taskQuery.getProcessInstanceId() != null) {
            filters.add(Filters.eq("processInstanceId", taskQuery.getProcessInstanceId()));
        }
        if (taskQuery.getAssignee() != null) {
            filters.add(Filters.eq("assignee", taskQuery.getAssignee()));
        }
        if (taskQuery.getUnassigned()) {
            filters.add(Filters.eq("assignee", null));
        }
        if (taskQuery.getName() != null) {
            filters.add(Filters.eq("name", taskQuery.getName()));
        }

        return filters.isEmpty() ? new BasicDBObject() : Filters.and(filters);
    }

    protected Bson createSort(TaskQueryImpl taskQuery) {
        List<Bson> bsonSorts = new ArrayList<>();
        for (Map.Entry<String, Boolean> entry : taskQuery.getOrderByColumnMap().entrySet()) {
            String column = entry.getKey();
            boolean ascending = entry.getValue();

            String columnName = null;
            if (TaskQueryProperty.NAME.getName().equals(column)) {
                columnName = "name";
            }

            if (columnName != null) {
                bsonSorts.add(ascending ? Sorts.ascending(columnName) : Sorts.descending(columnName));
            }
        }
        return bsonSorts.isEmpty() ? null : Sorts.orderBy(bsonSorts);
    }
}
