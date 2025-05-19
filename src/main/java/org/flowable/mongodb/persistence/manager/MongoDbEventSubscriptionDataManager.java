/* Licensed under the Apache License, Version 2.0 (the "License"); ... */

package org.flowable.mongodb.persistence.manager;

import java.util.ArrayList;
import java.util.List;

import org.bson.conversions.Bson;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.eventsubscription.api.EventSubscription;
import org.flowable.eventsubscription.service.impl.EventSubscriptionQueryImpl;
import org.flowable.eventsubscription.service.impl.persistence.entity.*;
import org.flowable.eventsubscription.service.impl.persistence.entity.data.EventSubscriptionDataManager;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;

public class MongoDbEventSubscriptionDataManager extends AbstractMongoDbDataManager<EventSubscriptionEntity> implements EventSubscriptionDataManager {

    public static final String COLLECTION_EVENT_SUBSCRIPTION = "eventSubscriptions";

    @Override
    public String getCollection() {
        return COLLECTION_EVENT_SUBSCRIPTION;
    }

    @Override
    public EventSubscriptionEntity create() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MessageEventSubscriptionEntity createMessageEventSubscription() {
        return new MessageEventSubscriptionEntityImpl();
    }

    @Override
    public SignalEventSubscriptionEntity createSignalEventSubscription() {
        return new SignalEventSubscriptionEntityImpl();
    }

    @Override
    public CompensateEventSubscriptionEntity createCompensateEventSubscription() {
        return new CompensateEventSubscriptionEntityImpl();
    }

    @Override
    public GenericEventSubscriptionEntity createGenericEventSubscriptionEntity() {
        return new GenericEventSubscriptionEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        return null;
    }

    @Override
    public long findEventSubscriptionCountByQueryCriteria(EventSubscriptionQueryImpl query) {
        List<Bson> filters = new ArrayList<>();
        if (query.getProcessInstanceId() != null) {
            filters.add(Filters.eq("processInstanceId", query.getProcessInstanceId()));
        }
        Bson combined = filters.isEmpty() ? null : Filters.and(filters);
        return getMongoDbSession().count(COLLECTION_EVENT_SUBSCRIPTION, combined);
    }

    @Override
    public List<EventSubscription> findEventSubscriptionsByQueryCriteria(EventSubscriptionQueryImpl query) {
        List<Bson> filters = new ArrayList<>();
        if (query.getProcessInstanceId() != null) {
            filters.add(Filters.eq("processInstanceId", query.getProcessInstanceId()));
        }
        Bson combined = filters.isEmpty() ? null : Filters.and(filters);
        return getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, combined);
    }

    @Override
    public List<MessageEventSubscriptionEntity> findMessageEventSubscriptionsByProcessInstanceAndEventName(String processInstanceId, String eventName) {
        Bson filter = Filters.and(
                Filters.eq("eventType", "message"),
                Filters.eq("processInstanceId", processInstanceId),
                Filters.eq("eventName", eventName)
        );
        return getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, filter);
    }

    @Override
    public List<SignalEventSubscriptionEntity> findSignalEventSubscriptionsByEventName(String eventName, String tenantId) {
        Bson filter = Filters.and(
                Filters.eq("eventType", "signal"),
                Filters.eq("eventName", eventName),
                Filters.eq("tenantId", tenantId)
        );
        return getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, filter);
    }

    @Override
    public List<SignalEventSubscriptionEntity> findSignalEventSubscriptionsByProcessInstanceAndEventName(String processInstanceId, String eventName) {
        Bson filter = Filters.and(
                Filters.eq("eventType", "signal"),
                Filters.eq("processInstanceId", processInstanceId),
                Filters.eq("eventName", eventName)
        );
        return getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, filter);
    }

    @Override
    public List<SignalEventSubscriptionEntity> findSignalEventSubscriptionsByNameAndExecution(String name, String executionId) {
        Bson filter = Filters.and(
                Filters.eq("eventType", "signal"),
                Filters.eq("eventName", name),
                Filters.eq("executionId", executionId)
        );
        return getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, filter);
    }

    @Override
    public List<EventSubscriptionEntity> findEventSubscriptionsByExecutionAndType(String executionId, String type) {
        Bson filter = Filters.and(
                Filters.eq("executionId", executionId),
                Filters.eq("eventType", type)
        );
        return getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, filter);
    }

    @Override
    public List<EventSubscriptionEntity> findEventSubscriptionsByProcessInstanceAndActivityId(String processInstanceId, String activityId, String type) {
        Bson filter = Filters.and(
                Filters.eq("processInstanceId", processInstanceId),
                Filters.eq("activityId", activityId),
                Filters.eq("eventType", type)
        );
        return getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, filter);
    }

    @Override
    public List<EventSubscriptionEntity> findEventSubscriptionsByExecution(String executionId) {
        return getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, Filters.eq("executionId", executionId));
    }

    @Override
    public List<EventSubscriptionEntity> findEventSubscriptionsByTypeAndProcessDefinitionId(String type, String processDefinitionId, String tenantId) {
        Bson filter = Filters.and(
                Filters.eq("eventType", type),
                Filters.eq("processDefinitionId", processDefinitionId),
                Filters.eq("tenantId", tenantId)
        );
        return getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, filter);
    }

    @Override
    public List<EventSubscriptionEntity> findEventSubscriptionsByName(String type, String eventName, String tenantId) {
        Bson filter = Filters.and(
                Filters.eq("eventType", type),
                Filters.eq("eventName", eventName),
                Filters.eq("tenantId", tenantId)
        );
        return getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, filter);
    }

    @Override
    public List<EventSubscriptionEntity> findEventSubscriptionsByNameAndExecution(String type, String eventName, String executionId) {
        Bson filter = Filters.and(
                Filters.eq("eventType", type),
                Filters.eq("eventName", eventName),
                Filters.eq("executionId", executionId)
        );
        return getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, filter);
    }

    @Override
    public MessageEventSubscriptionEntity findMessageStartEventSubscriptionByName(String messageName, String tenantId) {
        Bson filter = Filters.and(
                Filters.eq("eventType", "message"),
                Filters.eq("eventName", messageName),
                Filters.eq("tenantId", tenantId),
                Filters.exists("executionId", false)
        );
        List<MessageEventSubscriptionEntity> results = getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, filter);
        return results.isEmpty() ? null : results.get(0);
    }

    @Override
    public List<SignalEventSubscriptionEntity> findSignalEventSubscriptionsByScopeAndEventName(String scopeId, String scopeType, String eventName) {
        Bson filter = Filters.and(
                Filters.eq("scopeId", scopeId),
                Filters.eq("scopeType", scopeType),
                Filters.eq("eventType", "signal"),
                Filters.eq("eventName", eventName)
        );
        return getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, filter);
    }

    @Override
    public List<EventSubscriptionEntity> findEventSubscriptionsBySubScopeId(String subScopeId) {
        return getMongoDbSession().find(COLLECTION_EVENT_SUBSCRIPTION, Filters.eq("subScopeId", subScopeId));
    }

    @Override
    public void deleteEventSubscriptionsForScopeIdAndType(String scopeId, String scopeType) {
        Bson filter = Filters.and(
                Filters.eq("scopeId", scopeId),
                Filters.eq("scopeType", scopeType)
        );
        getMongoDbSession().bulkDelete(COLLECTION_EVENT_SUBSCRIPTION, filter);
    }

    @Override
    public void deleteEventSubscriptionsForScopeDefinitionIdAndType(String scopeDefinitionId, String scopeType) {
        Bson filter = Filters.and(
                Filters.eq("scopeDefinitionId", scopeDefinitionId),
                Filters.eq("scopeType", scopeType)
        );
        getMongoDbSession().bulkDelete(COLLECTION_EVENT_SUBSCRIPTION, filter);
    }

    @Override
    public void updateEventSubscriptionTenantId(String oldTenantId, String newTenantId) {
        List<EventSubscriptionEntity> subs = getMongoDbSession().find(
                COLLECTION_EVENT_SUBSCRIPTION, Filters.eq("tenantId", oldTenantId));
        for (EventSubscriptionEntity sub : subs) {
            sub.setTenantId(newTenantId);
            getMongoDbSession().update(sub);
        }
    }

    @Override
    public void deleteEventSubscriptionsForProcessDefinition(String processDefinitionId) {
        getMongoDbSession().bulkDelete(COLLECTION_EVENT_SUBSCRIPTION, Filters.and(
                Filters.eq("processDefinitionId", processDefinitionId),
                Filters.not(Filters.exists("executionId")),
                Filters.not(Filters.exists("processInstanceId"))
        ));
    }

    @Override
    public void deleteEventSubscriptionsByExecutionId(String executionId) {
        List<EventSubscriptionEntity> list = findEventSubscriptionsByExecution(executionId);
        for (EventSubscriptionEntity e : list) {
            delete(e);
        }
    }
}
