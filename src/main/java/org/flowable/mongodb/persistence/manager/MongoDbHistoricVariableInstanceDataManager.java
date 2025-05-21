package org.flowable.mongodb.persistence.manager;

import java.util.*;
import java.util.stream.Collectors;

import org.bson.conversions.Bson;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.variable.api.history.HistoricVariableInstance;
import org.flowable.variable.service.impl.HistoricVariableInstanceQueryImpl;
import org.flowable.variable.service.impl.persistence.entity.HistoricVariableInstanceEntity;
import org.flowable.variable.service.impl.persistence.entity.HistoricVariableInstanceEntityImpl;
import org.flowable.variable.service.impl.persistence.entity.data.HistoricVariableInstanceDataManager;
import org.flowable.variable.service.impl.persistence.entity.data.impl.cachematcher.HistoricVariableInstanceByProcInstMatcher;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;

public class MongoDbHistoricVariableInstanceDataManager extends AbstractMongoDbDataManager<HistoricVariableInstanceEntity> implements HistoricVariableInstanceDataManager {

    public static final String COLLECTION_HISTORIC_VARIABLE_INSTANCES = "historicVariableInstances";

    protected HistoricVariableInstanceByProcInstMatcher historicVariableInstanceByProcInstMatcher = new HistoricVariableInstanceByProcInstMatcher();

    @Override
    public String getCollection() {
        return COLLECTION_HISTORIC_VARIABLE_INSTANCES;
    }

    @Override
    public HistoricVariableInstanceEntity create() {
        return new HistoricVariableInstanceEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        HistoricVariableInstanceEntity var = (HistoricVariableInstanceEntity) entity;
        BasicDBObject update = new BasicDBObject();
        update.append("variableName", var.getVariableName());
        update.append("variableTypeName", var.getVariableTypeName());
        update.append("revision", var.getRevision());
        update.append("scopeId", var.getScopeId());
        update.append("subScopeId", var.getSubScopeId());
        update.append("scopeType", var.getScopeType());
        update.append("taskId", var.getTaskId());
        update.append("executionId", var.getExecutionId());
        update.append("processInstanceId", var.getProcessInstanceId());
        update.append("value", var.getValue());
        return update;
    }

    @Override
    public List<HistoricVariableInstanceEntity> findHistoricVariableInstancesByProcessInstanceId(String processInstanceId) {
        return getMongoDbSession().find(COLLECTION_HISTORIC_VARIABLE_INSTANCES, Filters.eq("processInstanceId", processInstanceId),
                processInstanceId, HistoricVariableInstanceEntityImpl.class, historicVariableInstanceByProcInstMatcher);
    }

    @Override
    public List<HistoricVariableInstanceEntity> findHistoricVariableInstancesByTaskId(String taskId) {
        return getMongoDbSession().find(COLLECTION_HISTORIC_VARIABLE_INSTANCES, Filters.eq("taskId", taskId));
    }

    @Override
    public long findHistoricVariableInstanceCountByQueryCriteria(HistoricVariableInstanceQueryImpl query) {
        return getMongoDbSession().count(COLLECTION_HISTORIC_VARIABLE_INSTANCES, createFilter(query));
    }

    @Override
    public List<HistoricVariableInstance> findHistoricVariableInstancesByQueryCriteria(HistoricVariableInstanceQueryImpl query) {
        return new ArrayList<>(getMongoDbSession().find(COLLECTION_HISTORIC_VARIABLE_INSTANCES, createFilter(query)));
    }

    @Override
    public HistoricVariableInstanceEntity findHistoricVariableInstanceByVariableInstanceId(String variableInstanceId) {
        return getMongoDbSession().findOne(COLLECTION_HISTORIC_VARIABLE_INSTANCES, Filters.eq("_id", variableInstanceId));
    }

    @Override
    public List<HistoricVariableInstanceEntity> findHistoricalVariableInstancesByScopeIdAndScopeType(String scopeId, String scopeType) {
        Bson filter = Filters.and(Filters.eq("scopeId", scopeId), Filters.eq("scopeType", scopeType));
        return getMongoDbSession().find(COLLECTION_HISTORIC_VARIABLE_INSTANCES, filter);
    }

    @Override
    public List<HistoricVariableInstanceEntity> findHistoricalVariableInstancesBySubScopeIdAndScopeType(String subScopeId, String scopeType) {
        Bson filter = Filters.and(Filters.eq("subScopeId", subScopeId), Filters.eq("scopeType", scopeType));
        return getMongoDbSession().find(COLLECTION_HISTORIC_VARIABLE_INSTANCES, filter);
    }

    @Override
    public List<HistoricVariableInstance> findHistoricVariableInstancesByNativeQuery(Map<String, Object> parameterMap) {
        BasicDBObject query = new BasicDBObject(parameterMap);
        return getMongoDbSession().find(COLLECTION_HISTORIC_VARIABLE_INSTANCES, query);
    }

    @Override
    public long findHistoricVariableInstanceCountByNativeQuery(Map<String, Object> parameterMap) {
        BasicDBObject query = new BasicDBObject(parameterMap);
        return getMongoDbSession().count(COLLECTION_HISTORIC_VARIABLE_INSTANCES, query);
    }

    @Override
    public void bulkDeleteHistoricVariableInstancesByProcessInstanceIds(Collection<String> processInstanceIds) {
        getMongoDbSession().bulkDelete(COLLECTION_HISTORIC_VARIABLE_INSTANCES, Filters.in("processInstanceId", processInstanceIds));
    }

    @Override
    public void bulkDeleteHistoricVariableInstancesByTaskIds(Collection<String> taskIds) {
        getMongoDbSession().bulkDelete(COLLECTION_HISTORIC_VARIABLE_INSTANCES, Filters.in("taskId", taskIds));
    }

    @Override
    public void bulkDeleteHistoricVariableInstancesByScopeIdsAndScopeType(Collection<String> scopeIds, String scopeType) {
        getMongoDbSession().bulkDelete(COLLECTION_HISTORIC_VARIABLE_INSTANCES,Filters.and(
                Filters.in("scopeId",scopeIds),
                Filters.eq("scopeType",scopeType)
        ));
    }

    @Override
    public void deleteHistoricVariableInstancesForNonExistingProcessInstances() {
    }

    @Override
    public void deleteHistoricVariableInstancesForNonExistingCaseInstances() {
    }

    protected Bson createFilter(HistoricVariableInstanceQueryImpl query) {
        List<Bson> filters = new ArrayList<>();
        if (query.getTaskId() != null) {
            filters.add(Filters.eq("taskId", query.getTaskId()));
        }
        if (query.getProcessInstanceId() != null) {
            filters.add(Filters.eq("processInstanceId", query.getProcessInstanceId()));
        }
        if (query.getScopeId() != null) {
            filters.add(Filters.eq("scopeId", query.getScopeId()));
        }
        if (query.getScopeType() != null) {
            filters.add(Filters.eq("scopeType", query.getScopeType()));
        }

        return filters.isEmpty() ? new BasicDBObject() : Filters.and(filters);
    }
}
