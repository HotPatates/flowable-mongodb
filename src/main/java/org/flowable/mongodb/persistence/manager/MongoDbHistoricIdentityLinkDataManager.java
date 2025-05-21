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

import java.util.Collection;
import java.util.List;

import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.identitylink.service.impl.persistence.entity.HistoricIdentityLinkEntity;
import org.flowable.identitylink.service.impl.persistence.entity.HistoricIdentityLinkEntityImpl;
import org.flowable.identitylink.service.impl.persistence.entity.data.HistoricIdentityLinkDataManager;
import org.flowable.identitylink.service.impl.persistence.entity.data.impl.cachematcher.HistoricIdentityLinksByProcInstMatcher;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;

public class MongoDbHistoricIdentityLinkDataManager extends AbstractMongoDbDataManager<HistoricIdentityLinkEntity> implements HistoricIdentityLinkDataManager {

    public static final String COLLECTION_HISTORIC_IDENTITY_LINKS = "historicIdentityLinks";

    protected HistoricIdentityLinksByProcInstMatcher historicIdentityLinksByProcInstMatcher = new HistoricIdentityLinksByProcInstMatcher();

    @Override
    public String getCollection() {
        return COLLECTION_HISTORIC_IDENTITY_LINKS;
    }

    @Override
    public HistoricIdentityLinkEntity create() {
        return new HistoricIdentityLinkEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        HistoricIdentityLinkEntity identityLink = (HistoricIdentityLinkEntity) entity;
        BasicDBObject update = new BasicDBObject();
        if (identityLink.getUserId() != null) update.append("userId", identityLink.getUserId());
        if (identityLink.getGroupId() != null) update.append("groupId", identityLink.getGroupId());
        if (identityLink.getTaskId() != null) update.append("taskId", identityLink.getTaskId());
        if (identityLink.getProcessInstanceId() != null) update.append("processInstanceId", identityLink.getProcessInstanceId());
        if (identityLink.getScopeId() != null) update.append("scopeId", identityLink.getScopeId());
        if (identityLink.getSubScopeId() != null) update.append("subScopeId", identityLink.getSubScopeId());
        if (identityLink.getScopeType() != null) update.append("scopeType", identityLink.getScopeType());
        if (identityLink.getScopeDefinitionId() != null) update.append("scopeDefinitionId", identityLink.getScopeDefinitionId());
        if (identityLink.getType() != null) update.append("type", identityLink.getType());
        return update;
    }

    @Override
    public List<HistoricIdentityLinkEntity> findHistoricIdentityLinksByTaskId(String taskId) {
        return getMongoDbSession().find(
                COLLECTION_HISTORIC_IDENTITY_LINKS,
                Filters.eq("taskId", taskId)
        );
    }

    @Override
    public List<HistoricIdentityLinkEntity> findHistoricIdentityLinksByProcessInstanceId(String processInstanceId) {
        return getMongoDbSession().find(
                COLLECTION_HISTORIC_IDENTITY_LINKS,
                Filters.eq("processInstanceId", processInstanceId),
                processInstanceId,
                HistoricIdentityLinkEntityImpl.class,
                historicIdentityLinksByProcInstMatcher
        );
    }

    @Override
    public List<HistoricIdentityLinkEntity> findHistoricIdentityLinksByScopeIdAndScopeType(String scopeId, String scopeType) {
        Bson filter = Filters.and(
                Filters.eq("scopeId", scopeId),
                Filters.eq("scopeType", scopeType)
        );
        return getMongoDbSession().find(COLLECTION_HISTORIC_IDENTITY_LINKS, filter);
    }

    @Override
    public List<HistoricIdentityLinkEntity> findHistoricIdentityLinksBySubScopeIdAndScopeType(String subScopeId, String scopeType) {
        Bson filter = Filters.and(
                Filters.eq("subScopeId", subScopeId),
                Filters.eq("scopeType", scopeType)
        );
        return getMongoDbSession().find(COLLECTION_HISTORIC_IDENTITY_LINKS, filter);
    }

    @Override
    public void deleteHistoricIdentityLinksByScopeIdAndType(String scopeId, String scopeType) {
        Bson filter = Filters.and(
                Filters.eq("scopeId", scopeId),
                Filters.eq("scopeType", scopeType)
        );
        getMongoDbSession().bulkDelete(COLLECTION_HISTORIC_IDENTITY_LINKS, filter);
    }

    @Override
    public void deleteHistoricIdentityLinksByScopeDefinitionIdAndType(String scopeDefinitionId, String scopeType) {
        Bson filter = Filters.and(
                Filters.eq("scopeDefinitionId", scopeDefinitionId),
                Filters.eq("scopeType", scopeType)
        );
        getMongoDbSession().bulkDelete(COLLECTION_HISTORIC_IDENTITY_LINKS, filter);
    }

    @Override
    public void bulkDeleteHistoricIdentityLinksForProcessInstanceIds(Collection<String> processInstanceIds) {
        getMongoDbSession().bulkDelete(COLLECTION_HISTORIC_IDENTITY_LINKS, Filters.in("processInstanceId", processInstanceIds));
    }

    @Override
    public void bulkDeleteHistoricIdentityLinksForTaskIds(Collection<String> taskIds) {
        getMongoDbSession().bulkDelete(COLLECTION_HISTORIC_IDENTITY_LINKS, Filters.in("taskId", taskIds));
    }

    @Override
    public void bulkDeleteHistoricIdentityLinksForScopeIdsAndScopeType(Collection<String> scopeIds, String scopeType) {
        getMongoDbSession().bulkDelete(COLLECTION_HISTORIC_IDENTITY_LINKS, Filters.and(Filters.in("scopeId", scopeIds), Filters.eq("scopeType", scopeType)));
    }

    @Override
    public void deleteHistoricProcessIdentityLinksForNonExistingInstances() {

    }

    @Override
    public void deleteHistoricCaseIdentityLinksForNonExistingInstances() {

    }

    @Override
    public void deleteHistoricTaskIdentityLinksForNonExistingInstances() {

    }
}
