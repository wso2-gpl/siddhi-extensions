package org.wso2.siddhi.gpl.extension.nlp;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.ListEvent;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.gpl.extension.nlp.dictionary.Dictionary;
import org.wso2.siddhi.gpl.extension.nlp.utility.Constants;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Created by malithi on 9/3/14.
 */
@SiddhiExtension(namespace = "nlp", function = "findNameEntityTypeViaDictionary")
public class NameEntityTypeViaDictionaryTransformProcessor extends TransformProcessor {

    private static Logger logger = Logger.getLogger(NameEntityTypeViaDictionaryTransformProcessor.class);

    private int inStreamParamPosition;
    private Constants.EntityType entityType;
    private Dictionary dictionary;

    @Override
    protected void init(Expression[] expressions, List<ExpressionExecutor> expressionExecutors, StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition, String elementId, SiddhiContext siddhiContext) {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing Query ...");
        }

        if (expressions.length < 3){
            throw new QueryCreationException("Query expects at least three parameters. Received only " +
                    expressions.length + ".\nUsage: findNameEntityTypeViaDictionary(entityType:string, " +
                    "dictionaryFilePath:string, text:string-variable)");
        }

        String entityTypeParam;
        try {
            entityTypeParam = ((StringConstant)expressions[0]).getValue();
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter entityType",e);
            throw new QueryCreationException("First parameter should be of type string. Found " + Constants.getType
                    (expressions[0]) + ".\nUsage: findNameEntityTypeViaDictionary(entityType:string, " +
                    "dictionaryFilePath:string, text:string-variable");
        }

        try {
            this.entityType = Constants.EntityType.valueOf(entityTypeParam.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.error("Entity Type ["+ entityTypeParam + "] is not defined", e);
            throw new QueryCreationException("First parameter should be one of " + Arrays.deepToString(Constants
                    .EntityType.values()) + ". Found " + entityTypeParam);
        }

        String dictionaryFilePath;
        try {
            dictionaryFilePath = ((StringConstant)expressions[1]).getValue();
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter dictionaryFilePath",e);
            throw new QueryCreationException("Second parameter should be of type string. Found " + Constants.getType
                    (expressions[0]) + ".\nUsage: findNameEntityTypeViaDictionary(entityType:string, " +
                    "dictionaryFilePath:string, text:string-variable");
        }

        try {
            dictionary = new Dictionary(entityType, dictionaryFilePath);
        } catch (Exception e) {
            logger.error("Error creating dictionary", e);
            throw new QueryCreationException("Failed to initialize dictionary. Error: [" + e.getMessage() + "]");
        }

        if (expressions[2] instanceof Variable){
            inStreamParamPosition = inStreamDefinition.getAttributePosition(((Variable)expressions[2])
                    .getAttributeName());
        }else{
            throw new QueryCreationException("Third parameter should be a variable. Found " + Constants.getType
                    (expressions[2]) + ".\nUsage: findNameEntityTypeViaDictionary(entityType:string, " +
                    "dictionaryFilePath:string, text:string-variable)");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. EntityType: %s DictionaryFilePath: %s " +
                            "Stream Parameters: %s", entityTypeParam, dictionaryFilePath,
                    inStreamDefinition.getAttributeList()));
        }

        if (outStreamDefinition == null) {
            this.outStreamDefinition = new StreamDefinition().name("nameEntityTypeViaDictionaryMatchStream");

            this.outStreamDefinition.attribute("match", Attribute.Type.STRING);

            for(Attribute strDef : inStreamDefinition.getAttributeList()) {
                this.outStreamDefinition.attribute(strDef.getName(), strDef.getType());
            }
        }
    }

    @Override
    protected InStream processEvent(InEvent inEvent) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Event received. Entity Type:%s DictionaryFilePath:%s Event:%s",
                    entityType.name(), dictionary.getXmlFilePath(), inEvent));
        }

        Object [] inStreamData = inEvent.getData();

        String text = (String)inEvent.getData(inStreamParamPosition);

        InListEvent transformedListEvent = new InListEvent();

        Set<String> dictionaryEntries = dictionary.getEntries(entityType);

        for (String entry:dictionaryEntries){
            if(text.contains(entry)){
                Object [] outStreamData = new Object[inStreamData.length + 1];
                outStreamData[0] = entry;
                System.arraycopy(inStreamData, 0, outStreamData, 1, inStreamData.length);
                transformedListEvent.addEvent(new InEvent(inEvent.getStreamId(), System.currentTimeMillis(),
                        outStreamData));
            }
        }

        return transformedListEvent;

    }

    @Override
    protected InStream processEvent(InListEvent inListEvent) {
        InListEvent transformedListEvent = new InListEvent();
        for (Event event : inListEvent.getEvents()) {
            if (event instanceof InEvent) {
                ListEvent resultListEvent = (ListEvent) processEvent((InEvent)event);
                transformedListEvent.setEvents(resultListEvent.getEvents());
            }
        }
        return transformedListEvent;
    }

    @Override
    protected Object[] currentState() {
        return new Object[0];
    }

    @Override
    protected void restoreState(Object[] objects) {

    }

    @Override
    public void destroy() {

    }

}
