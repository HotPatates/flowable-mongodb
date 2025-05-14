package org.flowable.mongodb.persistence.entity;

import org.flowable.engine.impl.persistence.entity.ProcessDefinitionInfoEntityImpl;

import java.util.Map;

public class MongoProcessDefinitionInfoEntity extends ProcessDefinitionInfoEntityImpl {

    protected boolean latest;

    @Override
    public Object getPersistentState() {
        Map<String, Object> persistentState = (Map<String, Object>) super.getPersistentState();
        persistentState.put("latest", latest);
        return persistentState;
    }

    public boolean isLatest() {
        return latest;
    }

    public void setLatest(boolean latest) {
        this.latest = latest;
    }


}
