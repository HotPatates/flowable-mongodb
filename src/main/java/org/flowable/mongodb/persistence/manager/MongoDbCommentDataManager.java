package org.flowable.mongodb.persistence.manager;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.engine.impl.persistence.entity.CommentEntity;
import org.flowable.engine.impl.persistence.entity.CommentEntityImpl;
import org.flowable.engine.impl.persistence.entity.data.CommentDataManager;
import org.flowable.engine.task.Comment;
import org.flowable.engine.task.Event;
import org.flowable.mongodb.cfg.MongoDbProcessEngineConfiguration;

import com.mongodb.BasicDBObject;

public class MongoDbCommentDataManager extends AbstractMongoDbDataManager<CommentEntity> implements CommentDataManager {

    public static final String COLLECTION_COMMENTS = "comments";
    public static final String COLLECTION_EVENTS = "events";

    public MongoDbCommentDataManager(MongoDbProcessEngineConfiguration processEngineConfiguration) {
        super(processEngineConfiguration);
    }

    @Override
    public String getCollection() {
        return COLLECTION_COMMENTS;
    }

    @Override
    public CommentEntity create() {
        return new CommentEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        CommentEntityImpl comment = (CommentEntityImpl) entity;
        BasicDBObject updateObject = null;
        updateObject = setUpdateProperty(comment, "type", comment.getType(), updateObject);
        updateObject = setUpdateProperty(comment, "userId", comment.getUserId(), updateObject);
        updateObject = setUpdateProperty(comment, "taskId", comment.getTaskId(), updateObject);
        updateObject = setUpdateProperty(comment, "processInstanceId", comment.getProcessInstanceId(), updateObject);
        updateObject = setUpdateProperty(comment, "time", comment.getTime(), updateObject);
        updateObject = setUpdateProperty(comment, "action", comment.getAction(), updateObject);
        updateObject = setUpdateProperty(comment, "message", comment.getMessage(), updateObject);
        updateObject = setUpdateProperty(comment, "fullMessage", comment.getFullMessage(), updateObject);
        return updateObject;
    }

    protected CommentEntityImpl mapToEntity(Document doc) {
        CommentEntityImpl entity = new CommentEntityImpl();
        entity.setId(doc.getString("id"));
        entity.setType(doc.getString("type"));
        entity.setUserId(doc.getString("userId"));
        entity.setTaskId(doc.getString("taskId"));
        entity.setProcessInstanceId(doc.getString("processInstanceId"));
        entity.setTime(doc.getDate("time"));
        entity.setAction(doc.getString("action"));
        entity.setMessage(doc.getString("message"));
        entity.setFullMessage(doc.getString("fullMessage"));
        return entity;
    }

    @Override
    public List<Comment> findCommentsByTaskId(String taskId) {
        List<Document> results = getMongoDbSession().find(COLLECTION_COMMENTS, new BasicDBObject("taskId", taskId));
        List<Comment> comments = new ArrayList<>();
        for (Document doc : results) {
            comments.add(mapToEntity(doc));
        }
        return comments;
    }

    @Override
    public List<Comment> findCommentsByTaskIdAndType(String taskId, String type) {
        BasicDBObject query = new BasicDBObject("taskId", taskId).append("type", type);
        List<Document> results = getMongoDbSession().find(COLLECTION_COMMENTS, query);
        List<Comment> comments = new ArrayList<>();
        for (Document doc : results) {
            comments.add(mapToEntity(doc));
        }
        return comments;
    }

    @Override
    public List<Comment> findCommentsByType(String type) {
        List<Document> results = getMongoDbSession().find(COLLECTION_COMMENTS, new BasicDBObject("type", type));
        List<Comment> comments = new ArrayList<>();
        for (Document doc : results) {
            comments.add(mapToEntity(doc));
        }
        return comments;
    }

    @Override
    public List<Event> findEventsByTaskId(String s) {
        return getMongoDbSession().find(COLLECTION_EVENTS, new Document("taskId", s));
    }

    @Override
    public List<Event> findEventsByProcessInstanceId(String s) {
        return getMongoDbSession().find(COLLECTION_EVENTS, new Document("processInstanceId", s));
    }

    @Override
    public void deleteCommentsByTaskId(String taskId) {
        findCommentsByTaskId(taskId).forEach(comment -> {
            delete(comment.getId());
        });
    }

    @Override
    public void deleteCommentsByProcessInstanceId(String processInstanceId) {
        findCommentsByProcessInstanceId(processInstanceId).forEach(comment -> {
            delete(comment.getId());
        });
    }

    @Override
    public List<Comment> findCommentsByProcessInstanceId(String processInstanceId) {
        List<Document> results = getMongoDbSession().find(COLLECTION_COMMENTS, new BasicDBObject("processInstanceId", processInstanceId));
        List<Comment> comments = new ArrayList<>();
        for (Document doc : results) {
            comments.add(mapToEntity(doc));
        }
        return comments;
    }

    @Override
    public List<Comment> findCommentsByProcessInstanceId(String processInstanceId, String type) {
        BasicDBObject query = new BasicDBObject("processInstanceId", processInstanceId).append("type", type);
        List<Document> results = getMongoDbSession().find(COLLECTION_COMMENTS, query);
        List<Comment> comments = new ArrayList<>();
        for (Document doc : results) {
            comments.add(mapToEntity(doc));
        }
        return comments;
    }

    @Override
    public Comment findComment(String commentId) {
        Document doc = getMongoDbSession().findOne(COLLECTION_COMMENTS, commentId);
        if (doc != null) {
            return mapToEntity(doc);
        }
        return null;
    }

    @Override
    public Event findEvent(String commentId) {
       return getMongoDbSession().findOne(COLLECTION_EVENTS, new Document("commentId", commentId));
    }

}
