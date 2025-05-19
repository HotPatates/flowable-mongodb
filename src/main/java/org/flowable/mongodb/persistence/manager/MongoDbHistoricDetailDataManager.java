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

import java.util.List;
import java.util.Map;

import org.bson.conversions.Bson;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.engine.history.HistoricDetail;
import org.flowable.engine.impl.HistoricDetailQueryImpl;
import org.flowable.engine.impl.persistence.entity.HistoricDetailAssignmentEntity;
import org.flowable.engine.impl.persistence.entity.HistoricDetailAssignmentEntityImpl;
import org.flowable.engine.impl.persistence.entity.HistoricDetailEntity;
import org.flowable.engine.impl.persistence.entity.HistoricDetailVariableInstanceUpdateEntity;
import org.flowable.engine.impl.persistence.entity.HistoricDetailVariableInstanceUpdateEntityImpl;
import org.flowable.engine.impl.persistence.entity.HistoricFormPropertyEntity;
import org.flowable.engine.impl.persistence.entity.HistoricFormPropertyEntityImpl;
import org.flowable.engine.impl.persistence.entity.data.HistoricDetailDataManager;
import org.flowable.mongodb.cfg.MongoDbProcessEngineConfiguration;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;

/**
 * @author Joram Barrez
 */
public class MongoDbHistoricDetailDataManager extends AbstractMongoDbDataManager<HistoricDetailEntity> implements HistoricDetailDataManager {

    public static final String COLLECTION_HISTORIC_DETAILS = "historicDetails";

    public MongoDbHistoricDetailDataManager(MongoDbProcessEngineConfiguration processEngineConfiguration) {
        super(processEngineConfiguration);
    }

    @Override
    public String getCollection() {
        return COLLECTION_HISTORIC_DETAILS;
    }

    @Override
    public HistoricDetailEntity create() {
        // Superclass is abstract
        throw new UnsupportedOperationException();
    }

    @Override
    public HistoricDetailAssignmentEntity createHistoricDetailAssignment() {
        return new HistoricDetailAssignmentEntityImpl();
    }

    @Override
    public HistoricDetailVariableInstanceUpdateEntity createHistoricDetailVariableInstanceUpdate() {
        return new HistoricDetailVariableInstanceUpdateEntityImpl();
    }

    @Override
    public HistoricFormPropertyEntity createHistoricFormProperty() {
        return new HistoricFormPropertyEntityImpl();
    }

    @Override
    public List<HistoricDetailEntity> findHistoricDetailsByProcessInstanceId(String processInstanceId) {
        return getMongoDbSession().find(COLLECTION_HISTORIC_DETAILS, Filters.eq("processInstanceId", processInstanceId));
    }

    @Override
    public List<HistoricDetailEntity> findHistoricDetailsByTaskId(String taskId) {
        return getMongoDbSession().find(COLLECTION_HISTORIC_DETAILS, Filters.eq("taskId", taskId));
    }

    @Override
    public void deleteHistoricDetailForNonExistingProcessInstances() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long findHistoricDetailCountByQueryCriteria(HistoricDetailQueryImpl historicVariableUpdateQuery) {
        return getMongoDbSession().count(COLLECTION_HISTORIC_DETAILS, createFilter(historicVariableUpdateQuery));
    }

    @Override
    public List<HistoricDetail> findHistoricDetailsByQueryCriteria(HistoricDetailQueryImpl historicVariableUpdateQuery) {
        return getMongoDbSession().find(COLLECTION_HISTORIC_DETAILS, createFilter(historicVariableUpdateQuery));
    }

    @Override
    public List<HistoricDetail> findHistoricDetailsByNativeQuery(Map<String, Object> parameterMap) {
        BasicDBObject query = new BasicDBObject(parameterMap);
        return getMongoDbSession().find(COLLECTION_HISTORIC_DETAILS, query);
    }

    @Override
    public long findHistoricDetailCountByNativeQuery(Map<String, Object> parameterMap) {
        BasicDBObject query = new BasicDBObject(parameterMap);
        return getMongoDbSession().count(COLLECTION_HISTORIC_DETAILS, query);
    }


    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        if (entity instanceof HistoricDetailVariableInstanceUpdateEntityImpl) {
            HistoricDetailVariableInstanceUpdateEntityImpl detail = (HistoricDetailVariableInstanceUpdateEntityImpl) entity;
            BasicDBObject update = new BasicDBObject();
            update.append("name", detail.getName());
            update.append("variableTypeName", detail.getVariableTypeName());
            update.append("textValue", detail.getTextValue());
            update.append("textValue2", detail.getTextValue2());
            update.append("longValue", detail.getLongValue());
            update.append("doubleValue", detail.getDoubleValue());
            update.append("processInstanceId", detail.getProcessInstanceId());
            update.append("executionId", detail.getExecutionId());
            update.append("taskId", detail.getTaskId());
            update.append("time", detail.getTime());
            update.append("revision", detail.getRevision());
            return update;

        } else if (entity instanceof HistoricFormPropertyEntityImpl) {
            HistoricFormPropertyEntityImpl detail = (HistoricFormPropertyEntityImpl) entity;
            BasicDBObject update = new BasicDBObject();
            update.append("propertyId", detail.getPropertyId());
            update.append("propertyValue", detail.getPropertyValue());
            update.append("processInstanceId", detail.getProcessInstanceId());
            update.append("executionId", detail.getExecutionId());
            update.append("taskId", detail.getTaskId());
            update.append("time", detail.getTime());
            return update;

        } else if (entity instanceof HistoricDetailAssignmentEntityImpl) {
            HistoricDetailAssignmentEntityImpl detail = (HistoricDetailAssignmentEntityImpl) entity;
            BasicDBObject update = new BasicDBObject();
            update.append("processInstanceId", detail.getProcessInstanceId());
            update.append("executionId", detail.getExecutionId());
            update.append("taskId", detail.getTaskId());
            update.append("time", detail.getTime());
            return update;
        }

        return null;
    }

    protected Bson createFilter(HistoricDetailQueryImpl query) {
        List<Bson> filters = new java.util.ArrayList<>();

        if (query.getProcessInstanceId() != null) {
            filters.add(Filters.eq("processInstanceId", query.getProcessInstanceId()));
        }

        if (query.getExecutionId() != null) {
            filters.add(Filters.eq("executionId", query.getExecutionId()));
        }

        if (query.getTaskId() != null) {
            filters.add(Filters.eq("taskId", query.getTaskId()));
        }


        return makeAndFilter(filters);
    }


}
