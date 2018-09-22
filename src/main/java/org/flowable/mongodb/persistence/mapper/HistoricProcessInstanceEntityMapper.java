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
package org.flowable.mongodb.persistence.mapper;

import org.bson.Document;
import org.flowable.engine.impl.persistence.entity.HistoricProcessInstanceEntityImpl;
import org.flowable.mongodb.persistence.EntityToDocumentMapper;

/**
 * @author Tijs Rademakers
 */
public class HistoricProcessInstanceEntityMapper extends AbstractEntityToDocumentMapper<HistoricProcessInstanceEntityImpl> {

    @Override
    public HistoricProcessInstanceEntityImpl fromDocument(Document document) {
        HistoricProcessInstanceEntityImpl instanceEntity = new HistoricProcessInstanceEntityImpl();
        instanceEntity.setId(document.getString("_id"));
        instanceEntity.setProcessInstanceId(document.getString("processInstanceId"));
        instanceEntity.setBusinessKey(document.getString("businessKey"));
        instanceEntity.setProcessDefinitionId(document.getString("processDefinitionId"));
        instanceEntity.setCallbackId(document.getString("callbackId"));
        instanceEntity.setCallbackType(document.getString("callbackType"));
        instanceEntity.setDeleteReason(document.getString("deleteReason"));
        instanceEntity.setDurationInMillis(document.getLong("durationInMillis"));
        instanceEntity.setEndActivityId(document.getString("endActivityId"));
        instanceEntity.setEndTime(document.getDate("endTime"));
        instanceEntity.setName(document.getString("name"));
        instanceEntity.setRevision(document.getInteger("revision"));
        instanceEntity.setStartActivityId(document.getString("startActivityId"));
        instanceEntity.setStartTime(document.getDate("startTime"));
        instanceEntity.setStartUserId(document.getString("startUserId"));
        instanceEntity.setSuperProcessInstanceId(document.getString("superProccessInstanceId"));
        instanceEntity.setTenantId(document.getString("tenantId"));
        
        return instanceEntity;
    }

    @Override
    public Document toDocument(HistoricProcessInstanceEntityImpl instanceEntity) {
        Document historicProcessInstanceDocument = new Document();
        appendIfNotNull(historicProcessInstanceDocument, "_id", instanceEntity.getId());
        appendIfNotNull(historicProcessInstanceDocument, "processInstanceId", instanceEntity.getProcessInstanceId());
        appendIfNotNull(historicProcessInstanceDocument, "businessKey", instanceEntity.getBusinessKey());
        appendIfNotNull(historicProcessInstanceDocument, "processDefinitionId", instanceEntity.getProcessDefinitionId());
        appendIfNotNull(historicProcessInstanceDocument, "callbackId", instanceEntity.getCallbackId());
        appendIfNotNull(historicProcessInstanceDocument, "callbackType", instanceEntity.getCallbackType());
        appendIfNotNull(historicProcessInstanceDocument, "deleteReason", instanceEntity.getDeleteReason());
        appendIfNotNull(historicProcessInstanceDocument, "durationInMillis", instanceEntity.getDurationInMillis());
        appendIfNotNull(historicProcessInstanceDocument, "endActivityId", instanceEntity.getEndActivityId());
        appendIfNotNull(historicProcessInstanceDocument, "endTime", instanceEntity.getEndTime());
        appendIfNotNull(historicProcessInstanceDocument, "name", instanceEntity.getName());
        appendIfNotNull(historicProcessInstanceDocument, "revision", instanceEntity.getRevision());
        appendIfNotNull(historicProcessInstanceDocument, "startActivityId", instanceEntity.getStartActivityId());
        appendIfNotNull(historicProcessInstanceDocument, "startTime", instanceEntity.getStartTime());
        appendIfNotNull(historicProcessInstanceDocument, "startUserId", instanceEntity.getStartUserId());
        appendIfNotNull(historicProcessInstanceDocument, "superProccessInstanceId", instanceEntity.getSuperProcessInstanceId());
        appendIfNotNull(historicProcessInstanceDocument, "tenantId", instanceEntity.getTenantId());
        
        return historicProcessInstanceDocument;
    }

}
