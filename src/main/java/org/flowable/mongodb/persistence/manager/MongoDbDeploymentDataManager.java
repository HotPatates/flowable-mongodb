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
import java.util.stream.Collectors;

import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.engine.impl.DeploymentQueryImpl;
import org.flowable.engine.impl.persistence.entity.DeploymentEntity;
import org.flowable.engine.impl.persistence.entity.DeploymentEntityImpl;
import org.flowable.engine.impl.persistence.entity.data.DeploymentDataManager;
import org.flowable.engine.repository.Deployment;
import org.flowable.mongodb.cfg.MongoDbProcessEngineConfiguration;

import com.mongodb.BasicDBObject;

/**
 * @author Joram Barrez
 */
public class MongoDbDeploymentDataManager extends AbstractMongoDbDataManager<DeploymentEntity> implements DeploymentDataManager {

    public static final String COLLECTION_DEPLOYMENT = "deployments";

    public MongoDbDeploymentDataManager(MongoDbProcessEngineConfiguration processEngineConfiguration) {
        super(processEngineConfiguration);
    }

    @Override
    public String getCollection() {
        return COLLECTION_DEPLOYMENT;
    }

    @Override
    public DeploymentEntity create() {
        return new DeploymentEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        DeploymentEntity deployment = (DeploymentEntity) entity;
        BasicDBObject update = new BasicDBObject();
        update.put("name", deployment.getName());
        update.put("category", deployment.getCategory());
        update.put("deploymentTime", deployment.getDeploymentTime());
        update.put("tenantId", deployment.getTenantId());
        update.put("engineVersion", deployment.getEngineVersion());
        return update;
    }


    @Override
    public long findDeploymentCountByQueryCriteria(DeploymentQueryImpl deploymentQuery) {
        BasicDBObject query = new BasicDBObject();

        if (deploymentQuery.getDeploymentId() != null) {
            query.put("deploymentId", deploymentQuery.getDeploymentId());
        }
        if (deploymentQuery.getName() != null) {
            query.put("name", deploymentQuery.getName());
        }
        if (deploymentQuery.getCategory() != null) {
            query.put("category", deploymentQuery.getCategory());
        }
        if (deploymentQuery.getTenantId() != null) {
            query.put("tenantId", deploymentQuery.getTenantId());
        }
        if (deploymentQuery.getNameLike() != null) {
            query.put("name", new BasicDBObject("$regex", deploymentQuery.getNameLike()));
        }

        return getMongoDbSession().count(COLLECTION_DEPLOYMENT, query);
    }


    @Override
    public List<Deployment> findDeploymentsByQueryCriteria(DeploymentQueryImpl deploymentQuery) {
        BasicDBObject query = new BasicDBObject();

        if (deploymentQuery.getDeploymentId() != null) {
            query.put("deploymentId", deploymentQuery.getDeploymentId());
        }
        if (deploymentQuery.getName() != null) {
            query.put("name", deploymentQuery.getName());
        }
        if (deploymentQuery.getCategory() != null) {
            query.put("category", deploymentQuery.getCategory());
        }
        if (deploymentQuery.getTenantId() != null) {
            query.put("tenantId", deploymentQuery.getTenantId());
        }
        if (deploymentQuery.getNameLike() != null) {
            query.put("name", new BasicDBObject("$regex", deploymentQuery.getNameLike()));
        }

        return getMongoDbSession().find(COLLECTION_DEPLOYMENT, query);
    }

    @Override
    public List<String> getDeploymentResourceNames(String deploymentId) {
        List<BasicDBObject> resources = getMongoDbSession().find("resources", new BasicDBObject("deploymentId", deploymentId));
        return resources.stream()
                .map(doc -> (String) doc.get("name"))
                .collect(Collectors.toList());
    }
    public List<Deployment> findDeploymentsByNativeQuery(Map<String, Object> parameterMap) {
        BasicDBObject query = new BasicDBObject(parameterMap);
        return getMongoDbSession().find(COLLECTION_DEPLOYMENT, query);
    }

    public long findDeploymentCountByNativeQuery(Map<String, Object> parameterMap) {
        BasicDBObject query = new BasicDBObject(parameterMap);
        return getMongoDbSession().count(COLLECTION_DEPLOYMENT, query);
    }

}
