package org.flowable.mongodb.persistence.manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.flowable.common.engine.api.FlowableIllegalArgumentException;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.variable.service.impl.InternalVariableInstanceQueryImpl;
import org.flowable.variable.service.impl.persistence.entity.VariableInstanceEntity;
import org.flowable.variable.service.impl.persistence.entity.VariableInstanceEntityImpl;
import org.flowable.variable.service.impl.persistence.entity.data.VariableInstanceDataManager;
import org.flowable.variable.service.impl.persistence.entity.data.impl.cachematcher.VariableInstanceByExecutionIdMatcher;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;

public class MongoDbVariableInstanceDataManager extends AbstractMongoDbDataManager<VariableInstanceEntity> implements VariableInstanceDataManager {

    public static final String COLLECTION_VARIABLES = "variables";

    protected VariableInstanceByExecutionIdMatcher variableInstanceByExecutionIdMatcher = new VariableInstanceByExecutionIdMatcher();

    @Override
    public String getCollection() {
        return COLLECTION_VARIABLES;
    }

    @Override
    public VariableInstanceEntity create() {
        return new VariableInstanceEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        VariableInstanceEntity variableEntity = (VariableInstanceEntity) entity;
        BasicDBObject updateObject = null;
        updateObject = setUpdateProperty(variableEntity, "textValue", variableEntity.getTextValue(), updateObject);
        updateObject = setUpdateProperty(variableEntity, "textValue2", variableEntity.getTextValue2(), updateObject);
        updateObject = setUpdateProperty(variableEntity, "doubleValue", variableEntity.getDoubleValue(), updateObject);
        updateObject = setUpdateProperty(variableEntity, "longValue", variableEntity.getLongValue(), updateObject);
        updateObject = setUpdateProperty(variableEntity, "typeName", variableEntity.getTypeName(), updateObject);
        return updateObject;
    }

    @Override
    public List<VariableInstanceEntity> findVariablesInstancesByQuery(InternalVariableInstanceQueryImpl query) {
        List<Bson> filters = new ArrayList<>();

        if (query.getName() != null) {
            filters.add(Filters.eq("name", query.getName()));
        }

        if (query.getScopeId() != null && query.getScopeType() != null) {
            filters.add(Filters.eq("scopeId", query.getScopeId()));
            filters.add(Filters.eq("scopeType", query.getScopeType()));
        }

        if (query.getTaskId() != null) {
            filters.add(Filters.eq("taskId", query.getTaskId()));
        }

        if (query.getExecutionId() != null) {
            filters.add(Filters.eq("executionId", query.getExecutionId()));
        }

        Bson finalFilter = filters.isEmpty() ? null : Filters.and(filters);
        return getMongoDbSession().find(COLLECTION_VARIABLES, finalFilter);
    }

    @Override
    public VariableInstanceEntity findVariablesInstanceByQuery(InternalVariableInstanceQueryImpl query) {
        List<VariableInstanceEntity> results = findVariablesInstancesByQuery(query);
        if (results.size() > 1) {
            throw new FlowableIllegalArgumentException("Query returned more than one result: " + results);
        }
        return results.isEmpty() ? null : results.get(0);
    }

    @Override
    public void deleteVariablesByTaskId(String taskId) {
        Bson filter = Filters.eq("taskId", taskId);
        getMongoDbSession().bulkDelete(COLLECTION_VARIABLES, filter);
    }

    protected List<VariableInstanceEntity> findVariableInstancesByExecutionId(String executionId) {
        return getMongoDbSession().find(COLLECTION_VARIABLES, Filters.eq("executionId", executionId), executionId,
                VariableInstanceEntityImpl.class, variableInstanceByExecutionIdMatcher, true);
    }

    @Override
    public void deleteVariablesByExecutionId(String executionId) {
        List<VariableInstanceEntity> variables = findVariableInstancesByExecutionId(executionId);
        if (variables != null) {
            for (VariableInstanceEntity variable : variables) {
                getMongoDbSession().delete(COLLECTION_VARIABLES, variable);
            }
        }
    }

    @Override
    public void deleteByScopeIdAndScopeType(String scopeId, String scopeType) {
        Bson filter = Filters.and(Filters.eq("scopeId", scopeId), Filters.eq("scopeType", scopeType));
        getMongoDbSession().bulkDelete(COLLECTION_VARIABLES, filter);
    }

    @Override
    public void deleteByScopeIdAndScopeTypes(String scopeId, Collection<String> scopeTypes) {
        Bson filter = Filters.and(
                Filters.eq("scopeId", scopeId),
                Filters.in("scopeType", scopeTypes)
        );
        getMongoDbSession().bulkDelete(COLLECTION_VARIABLES, filter);
    }

    @Override
    public void deleteBySubScopeIdAndScopeTypes(String subScopeId, Collection<String> scopeTypes) {
        Bson filter = Filters.and(
                Filters.eq("subScopeId", subScopeId),
                Filters.in("scopeType", scopeTypes)
        );
        getMongoDbSession().bulkDelete(COLLECTION_VARIABLES, filter);
    }

    public VariableInstanceEntityImpl transformToEntity(Document document) {
        VariableInstanceEntityImpl variable = new VariableInstanceEntityImpl();
        variable.setId(document.getString("_id"));
        variable.setName(document.getString("name"));
        variable.setTaskId(document.getString("taskId"));
        variable.setExecutionId(document.getString("executionId"));
        variable.setProcessInstanceId(document.getString("processInstanceId"));
        variable.setScopeId(document.getString("scopeId"));
        variable.setSubScopeId(document.getString("subScopeId"));
        variable.setScopeType(document.getString("scopeType"));
        variable.setTypeName(document.getString("typeName"));
        variable.setTextValue(document.getString("textValue"));
        variable.setTextValue2(document.getString("textValue2"));
        variable.setDoubleValue(document.getDouble("doubleValue"));
        variable.setLongValue(document.getLong("longValue"));
        return variable;
    }
}
