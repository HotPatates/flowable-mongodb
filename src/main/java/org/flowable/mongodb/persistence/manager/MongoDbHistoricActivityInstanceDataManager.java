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
import org.flowable.engine.ProcessEngineConfiguration;
import org.flowable.engine.history.HistoricActivityInstance;
import org.flowable.engine.impl.HistoricActivityInstanceQueryImpl;
import org.flowable.engine.impl.persistence.entity.HistoricActivityInstanceEntity;
import org.flowable.engine.impl.persistence.entity.HistoricActivityInstanceEntityImpl;
import org.flowable.engine.impl.persistence.entity.data.HistoricActivityInstanceDataManager;
import org.flowable.engine.runtime.ActivityInstance;
import org.flowable.mongodb.cfg.MongoDbProcessEngineConfiguration;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;

/**
 * @author Tijs Rademakers
 * @author Joram Barrez
 * @author Patata
 */
/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 */

public class MongoDbHistoricActivityInstanceDataManager extends AbstractMongoDbDataManager<HistoricActivityInstanceEntity> implements HistoricActivityInstanceDataManager {

    public static final String COLLECTION_HISTORIC_ACTIVITY_INSTANCES = "historicActivityInstances";

    public MongoDbHistoricActivityInstanceDataManager(MongoDbProcessEngineConfiguration processEngineConfiguration) {
        super(processEngineConfiguration);
    }

    @Override
    public String getCollection() {
        return COLLECTION_HISTORIC_ACTIVITY_INSTANCES;
    }

    @Override
    public HistoricActivityInstanceEntity create() {
        return new HistoricActivityInstanceEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        HistoricActivityInstanceEntity historicActivity = (HistoricActivityInstanceEntity) entity;
        BasicDBObject update = new BasicDBObject();
        update.append("executionId", historicActivity.getExecutionId());
        update.append("processInstanceId", historicActivity.getProcessInstanceId());
        update.append("processDefinitionId", historicActivity.getProcessDefinitionId());
        update.append("activityId", historicActivity.getActivityId());
        update.append("activityName", historicActivity.getActivityName());
        update.append("activityType", historicActivity.getActivityType());
        update.append("assignee", historicActivity.getAssignee());
        update.append("startTime", historicActivity.getStartTime());
        update.append("endTime", historicActivity.getEndTime());
        update.append("durationInMillis", historicActivity.getDurationInMillis());
        update.append("deleteReason", historicActivity.getDeleteReason());
        update.append("tenantId", historicActivity.getTenantId());
        return update;
    }

    @Override
    public HistoricActivityInstanceEntity create(ActivityInstance activityInstance) {
        return new HistoricActivityInstanceEntityImpl(activityInstance);
    }

    @Override
    public List<HistoricActivityInstanceEntity> findUnfinishedHistoricActivityInstancesByExecutionAndActivityId(String executionId, String activityId) {
        Bson filter = Filters.and(Filters.eq("executionId", executionId), Filters.eq("activityId", activityId));
        return getMongoDbSession().find(COLLECTION_HISTORIC_ACTIVITY_INSTANCES, filter);
    }

    @Override
    public List<HistoricActivityInstanceEntity> findHistoricActivityInstancesByExecutionIdAndActivityId(String executionId, String activityId) {
        Bson filter = Filters.and(Filters.eq("executionId", executionId), Filters.eq("activityId", activityId));
        return getMongoDbSession().find(COLLECTION_HISTORIC_ACTIVITY_INSTANCES, filter);
    }

    @Override
    public List<HistoricActivityInstanceEntity> findUnfinishedHistoricActivityInstancesByProcessInstanceId(String processInstanceId) {
        Bson filter = Filters.and(Filters.eq("processInstanceId", processInstanceId), Filters.exists("endTime", false));
        return getMongoDbSession().find(COLLECTION_HISTORIC_ACTIVITY_INSTANCES, filter);
    }

    @Override
    public void deleteHistoricActivityInstancesByProcessInstanceId(String historicProcessInstanceId) {
        getMongoDbSession().bulkDelete(COLLECTION_HISTORIC_ACTIVITY_INSTANCES, Filters.eq("processInstanceId", historicProcessInstanceId));
    }

    @Override
    public long findHistoricActivityInstanceCountByQueryCriteria(HistoricActivityInstanceQueryImpl query) {
        return getMongoDbSession().count(COLLECTION_HISTORIC_ACTIVITY_INSTANCES, createFilter(query));
    }

    @Override
    public List<HistoricActivityInstance> findHistoricActivityInstancesByQueryCriteria(HistoricActivityInstanceQueryImpl query) {
        return getMongoDbSession().find(COLLECTION_HISTORIC_ACTIVITY_INSTANCES, createFilter(query));
    }

    @Override
    public List<HistoricActivityInstance> findHistoricActivityInstancesByNativeQuery(Map<String, Object> parameterMap) {
        BasicDBObject filter = new BasicDBObject(parameterMap);
        return getMongoDbSession().find(COLLECTION_HISTORIC_ACTIVITY_INSTANCES, filter);
    }

    @Override
    public long findHistoricActivityInstanceCountByNativeQuery(Map<String, Object> parameterMap) {
        BasicDBObject filter = new BasicDBObject(parameterMap);
        return getMongoDbSession().count(COLLECTION_HISTORIC_ACTIVITY_INSTANCES, filter);
    }

    @Override
    public void deleteHistoricActivityInstances(HistoricActivityInstanceQueryImpl query) {
        getMongoDbSession().bulkDelete(COLLECTION_HISTORIC_ACTIVITY_INSTANCES, createFilter(query));
    }

    @Override
    public void bulkDeleteHistoricActivityInstancesByProcessInstanceIds(Collection<String> historicProcessInstanceIds) {
        getMongoDbSession().bulkDelete(COLLECTION_HISTORIC_ACTIVITY_INSTANCES, Filters.in("processInstanceId", historicProcessInstanceIds));
    }

    @Override
    public void deleteHistoricActivityInstancesForNonExistingProcessInstances() {
        Bson filter = Filters.or(
                Filters.eq("processInstanceId", null),
                Filters.not(Filters.exists("processInstanceId"))
        );
        getMongoDbSession().bulkDelete(COLLECTION_HISTORIC_ACTIVITY_INSTANCES, filter);
    }

    protected Bson createFilter(HistoricActivityInstanceQueryImpl query) {
        List<Bson> filters = new ArrayList<>();
        if (query.getExecutionId() != null) filters.add(Filters.eq("executionId", query.getExecutionId()));
        if (query.getProcessInstanceId() != null) filters.add(Filters.eq("processInstanceId", query.getProcessInstanceId()));
        if (query.getActivityId() != null) filters.add(Filters.eq("activityId", query.getActivityId()));
        if (query.getActivityName() != null) filters.add(Filters.eq("activityName", query.getActivityName()));
        if (query.getProcessDefinitionId() != null) filters.add(Filters.eq("processDefinitionId", query.getProcessDefinitionId()));
        if (query.getActivityType() != null) filters.add(Filters.eq("activityType", query.getActivityType()));
        if (query.getAssignee() != null) filters.add(Filters.eq("assignee", query.getAssignee()));
        if (query.isFinished()) filters.add(Filters.exists("endTime"));
        if (query.isUnfinished()) filters.add(Filters.not(Filters.exists("endTime")));
        if (query.getDeleteReason() != null) filters.add(Filters.eq("deleteReason", query.getDeleteReason()));
        if (query.getDeleteReasonLike() != null) filters.add(Filters.regex("deleteReason", query.getDeleteReasonLike().replace("%", ".*")));
        if (query.getTenantId() != null) filters.add(Filters.eq("tenantId", query.getTenantId()));
        if (query.getTenantIdLike() != null) filters.add(Filters.regex("tenantId", query.getTenantIdLike().replace("%", ".*")));
        if (query.isWithoutTenantId()) {
            filters.add(Filters.or(Filters.eq("tenantId", ProcessEngineConfiguration.NO_TENANT_ID), Filters.not(Filters.exists("tenantId"))));
        }
        return makeAndFilter(filters);
    }
}

