package org.flowable.mongodb.persistence.manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.identitylink.api.history.HistoricIdentityLink;
import org.flowable.identitylink.service.impl.persistence.entity.IdentityLinkEntity;
import org.flowable.identitylink.service.impl.persistence.entity.IdentityLinkEntityImpl;
import org.flowable.identitylink.service.impl.persistence.entity.data.IdentityLinkDataManager;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;

/**
 * @author Joram Barrez
 */
public class MongoDbIdentityLinkDataManager extends AbstractMongoDbDataManager<IdentityLinkEntity> implements IdentityLinkDataManager {

    public static final String COLLECTION_IDENTITY_LINKS = "identityLinks";

    @Override
    public String getCollection() {
        return COLLECTION_IDENTITY_LINKS;
    }

    @Override
    public IdentityLinkEntity create() {
        return new IdentityLinkEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        IdentityLinkEntity link = (IdentityLinkEntity) entity;
        BasicDBObject update = new BasicDBObject();
        update.append("userId", link.getUserId());
        update.append("groupId", link.getGroupId());
        update.append("taskId", link.getTaskId());
        update.append("processInstanceId", link.getProcessInstanceId());
        update.append("processDefinitionId", link.getProcessDefinitionId());
        update.append("scopeId", link.getScopeId());
        update.append("subScopeId", link.getSubScopeId());
        update.append("scopeType", link.getScopeType());
        update.append("scopeDefinitionId", link.getScopeDefinitionId());
        update.append("type", link.getType());
        return update;
    }

    @Override
    public IdentityLinkEntity createIdentityLinkFromHistoricIdentityLink(HistoricIdentityLink historicIdentityLink) {
        return new IdentityLinkEntityImpl(historicIdentityLink);
    }

    @Override
    public List<IdentityLinkEntity> findIdentityLinksByTaskId(String taskId) {
        return getMongoDbSession().find(COLLECTION_IDENTITY_LINKS, Filters.eq("taskId", taskId));
    }

    @Override
    public List<IdentityLinkEntity> findIdentityLinksByProcessInstanceId(String processInstanceId) {
        return getMongoDbSession().find(COLLECTION_IDENTITY_LINKS, Filters.eq("processInstanceId", processInstanceId));
    }

    @Override
    public List<IdentityLinkEntity> findIdentityLinksByProcessDefinitionId(String processDefinitionId) {
        return getMongoDbSession().find(COLLECTION_IDENTITY_LINKS, Filters.eq("processDefinitionId", processDefinitionId));
    }

    @Override
    public List<IdentityLinkEntity> findIdentityLinksByScopeIdAndType(String scopeId, String scopeType) {
        Bson filter = Filters.and(Filters.eq("scopeId", scopeId), Filters.eq("scopeType", scopeType));
        return getMongoDbSession().find(COLLECTION_IDENTITY_LINKS, filter);
    }

    @Override
    public List<IdentityLinkEntity> findIdentityLinksBySubScopeIdAndType(String subScopeId, String scopeType) {
        Bson filter = Filters.and(Filters.eq("subScopeId", subScopeId), Filters.eq("scopeType", scopeType));
        return getMongoDbSession().find(COLLECTION_IDENTITY_LINKS, filter);
    }

    @Override
    public List<IdentityLinkEntity> findIdentityLinksByScopeDefinitionIdAndType(String scopeDefinitionId, String scopeType) {
        Bson filter = Filters.and(Filters.eq("scopeDefinitionId", scopeDefinitionId), Filters.eq("scopeType", scopeType));
        return getMongoDbSession().find(COLLECTION_IDENTITY_LINKS, filter);
    }

    @Override
    public List<IdentityLinkEntity> findIdentityLinkByTaskUserGroupAndType(String taskId, String userId, String groupId, String type) {
        List<Bson> filters = new ArrayList<>();
        filters.add(Filters.eq("taskId", taskId));
        if (userId != null) {
            filters.add(Filters.eq("userId", userId));
        }
        if (groupId != null) {
            filters.add(Filters.eq("groupId", groupId));
        }
        if (type != null) {
            filters.add(Filters.eq("type", type));
        }
        return getMongoDbSession().find(COLLECTION_IDENTITY_LINKS, Filters.and(filters));
    }

    @Override
    public List<IdentityLinkEntity> findIdentityLinkByProcessInstanceUserGroupAndType(String processInstanceId, String userId, String groupId, String type) {
        List<Bson> filters = new ArrayList<>();
        filters.add(Filters.eq("processInstanceId", processInstanceId));
        if (userId != null) {
            filters.add(Filters.eq("userId", userId));
        }
        if (groupId != null) {
            filters.add(Filters.eq("groupId", groupId));
        }
        if (type != null) {
            filters.add(Filters.eq("type", type));
        }
        return getMongoDbSession().find(COLLECTION_IDENTITY_LINKS, Filters.and(filters));
    }

    @Override
    public List<IdentityLinkEntity> findIdentityLinkByProcessDefinitionUserAndGroup(String processDefinitionId, String userId, String groupId) {
        List<Bson> filters = new ArrayList<>();
        filters.add(Filters.eq("processDefinitionId", processDefinitionId));
        if (userId != null) {
            filters.add(Filters.eq("userId", userId));
        }
        if (groupId != null) {
            filters.add(Filters.eq("groupId", groupId));
        }
        return getMongoDbSession().find(COLLECTION_IDENTITY_LINKS, Filters.and(filters));
    }

    @Override
    public List<IdentityLinkEntity> findIdentityLinkByScopeIdScopeTypeUserGroupAndType(String scopeId, String scopeType, String userId, String groupId, String type) {
        List<Bson> filters = new ArrayList<>();
        filters.add(Filters.eq("scopeId", scopeId));
        filters.add(Filters.eq("scopeType", scopeType));
        if (userId != null) {
            filters.add(Filters.eq("userId", userId));
        }
        if (groupId != null) {
            filters.add(Filters.eq("groupId", groupId));
        }
        if (type != null) {
            filters.add(Filters.eq("type", type));
        }
        return getMongoDbSession().find(COLLECTION_IDENTITY_LINKS, Filters.and(filters));
    }

    @Override
    public List<IdentityLinkEntity> findIdentityLinkByScopeDefinitionScopeTypeUserAndGroup(String scopeDefinitionId, String scopeType, String userId, String groupId) {
        List<Bson> filters = new ArrayList<>();
        filters.add(Filters.eq("scopeDefinitionId", scopeDefinitionId));
        filters.add(Filters.eq("scopeType", scopeType));
        if (userId != null) {
            filters.add(Filters.eq("userId", userId));
        }
        if (groupId != null) {
            filters.add(Filters.eq("groupId", groupId));
        }
        return getMongoDbSession().find(COLLECTION_IDENTITY_LINKS, Filters.and(filters));
    }

    @Override
    public void deleteIdentityLinksByTaskId(String taskId) {
        getMongoDbSession().bulkDelete(COLLECTION_IDENTITY_LINKS, Filters.eq("taskId", taskId));
    }

    @Override
    public void deleteIdentityLinksByProcDef(String processDefinitionId) {
        getMongoDbSession().bulkDelete(COLLECTION_IDENTITY_LINKS, Filters.eq("processDefinitionId", processDefinitionId));
    }

    @Override
    public void deleteIdentityLinksByProcessInstanceId(String processInstanceId) {
        getMongoDbSession().bulkDelete(COLLECTION_IDENTITY_LINKS, Filters.eq("processInstanceId", processInstanceId));
    }

    @Override
    public void deleteIdentityLinksByScopeIdAndScopeType(String scopeId, String scopeType) {
        getMongoDbSession().bulkDelete(COLLECTION_IDENTITY_LINKS, Filters.and(
                Filters.eq("scopeId", scopeId),
                Filters.eq("scopeType", scopeType)
        ));
    }

    @Override
    public void deleteIdentityLinksByScopeDefinitionIdAndScopeType(String scopeDefinitionId, String scopeType) {
        getMongoDbSession().bulkDelete(COLLECTION_IDENTITY_LINKS, Filters.and(
                Filters.eq("scopeDefinitionId", scopeDefinitionId),
                Filters.eq("scopeType", scopeType)
        ));
    }

    @Override
    public void bulkDeleteIdentityLinksForScopeIdsAndScopeType(Collection<String> scopeIds, String scopeType) {
        getMongoDbSession().bulkDelete(COLLECTION_IDENTITY_LINKS, Filters.and(
                Filters.in("scopeId",scopeIds),
                Filters.eq("scopeType", scopeType)
        ));
    }
}
