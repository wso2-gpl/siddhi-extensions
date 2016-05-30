/*
 * Copyright (C) 2015 WSO2 Inc. (http://wso2.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.wso2.siddhi.gpl.extension.nlp;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.gpl.extension.nlp.dictionary.Dictionary;
import org.wso2.siddhi.gpl.extension.nlp.utility.Constants;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NameEntityTypeViaDictionaryStreamProcessor extends StreamProcessor {

    private static Logger logger = Logger.getLogger(NameEntityTypeViaDictionaryStreamProcessor.class);

    private Constants.EntityType entityType;
    private Dictionary dictionary;

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext, boolean outputExpectsExpiredEvents) {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing Query ...");
        }

        if (attributeExpressionLength < 3) {
            throw new ExecutionPlanCreationException("Query expects at least three parameters. Received only " +
                    attributeExpressionLength + ".\nUsage: #nlp:findNameEntityTypeViaDictionary(entityType:string, " +
                    "dictionaryFilePath:string, text:string-variable)");
        }

        String entityTypeParam;
        try {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                entityTypeParam = (String) attributeExpressionExecutors[0].execute(null);
            } else {
                throw new ExecutionPlanCreationException("First parameter should be a constant.");
            }
        } catch (ClassCastException e) {
            throw new ExecutionPlanCreationException("First parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].getReturnType() +
                    ".\nUsage: findNameEntityTypeViaDictionary(entityType:string, " +
                    "dictionaryFilePath:string, text:string-variable");
        }

        try {
            this.entityType = Constants.EntityType.valueOf(entityTypeParam.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ExecutionPlanCreationException("First parameter should be one of " + Arrays.deepToString(Constants
                    .EntityType.values()) + ". Found " + entityTypeParam);
        }

        String dictionaryFilePath;
        try {
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                dictionaryFilePath = (String) attributeExpressionExecutors[1].execute(null);
            } else {
                throw new ExecutionPlanCreationException("Second parameter should be a constant.");
            }
        } catch (ClassCastException e) {
            throw new ExecutionPlanCreationException("Second parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].getReturnType() +
                    ".\nUsage: findNameEntityTypeViaDictionary(entityType:string, " +
                    "dictionaryFilePath:string, text:string-variable");
        }

        try {
            dictionary = new Dictionary(entityType, dictionaryFilePath);
        } catch (Exception e) {
            throw new ExecutionPlanCreationException("Failed to initialize dictionary.", e);
        }

        if (!(attributeExpressionExecutors[2] instanceof VariableExpressionExecutor)) {
            throw new ExecutionPlanCreationException("Third parameter should be a variable. Found " +
                    attributeExpressionExecutors[2].getReturnType() +
                    ".\nUsage: findNameEntityTypeViaDictionary(entityType:string, " +
                    "dictionaryFilePath:string, text:string-variable)");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. EntityType: %s DictionaryFilePath: %s " +
                            "Stream Parameters: %s", entityTypeParam, dictionaryFilePath,
                    abstractDefinition.getAttributeList()));
        }

        ArrayList<Attribute> attributes = new ArrayList<Attribute>(1);
        attributes.add(new Attribute("match", Attribute.Type.STRING));
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Event received. Entity Type:%s DictionaryFilePath:%s Event:%s",
                        entityType.name(), dictionary.getXmlFilePath(), streamEvent));
            }

            String text = attributeExpressionExecutors[2].execute(streamEvent).toString();
            ArrayList<String> dictionaryEntries = dictionary.getEntries(entityType);

            if (dictionaryEntries.size() == 0) {
                streamEventChunk.remove();
            } else if (dictionaryEntries.size() == 1) {
                complexEventPopulater.populateComplexEvent(streamEvent, new Object[]{dictionaryEntries.get(0)});
            } else {
                for (String entry : dictionaryEntries) {
                    if (text.contains(entry)) {
                        StreamEvent newStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                        complexEventPopulater.populateComplexEvent(newStreamEvent, new Object[]{entry});
                        streamEventChunk.insertBeforeCurrent(newStreamEvent);
                    }
                }
                streamEventChunk.remove();
            }
        }

        nextProcessor.process(streamEventChunk);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] objects) {

    }
}
