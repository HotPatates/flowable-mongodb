package org.flowable.mongodb.cfg;

import org.flowable.common.engine.impl.interceptor.Command;
import org.flowable.common.engine.impl.interceptor.CommandContext;
import org.flowable.mongodb.schema.MongoProcessSchemaManager;

public class MongoSchemaManagementCommand implements Command<Void> {

    private final MongoProcessSchemaManager schemaManager;

    public MongoSchemaManagementCommand(MongoProcessSchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    @Override
    public Void execute(CommandContext commandContext) {
        schemaManager.schemaUpdate();
        return null;
    }
}