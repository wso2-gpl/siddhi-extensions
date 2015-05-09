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

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
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
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class TokensRegexPatternStreamProcessor extends StreamProcessor {

    private static Logger logger = Logger.getLogger(TokensRegexPatternStreamProcessor.class);

    private static final String groupPrefix = "group_";
    private int attributeCount;
    private TokenSequencePattern regexPattern;
    private StanfordCoreNLP pipeline;


    private void initPipeline() {
        logger.info("Initializing Annotator pipeline ...");
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse");

        pipeline = new StanfordCoreNLP(props);
        logger.info("Annotator pipeline initialized");
    }

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing Query ...");
        }

        if (attributeExpressionLength < 2) {
            throw new ExecutionPlanCreationException("Query expects at least two parameters. Received only " +
                    attributeExpressionLength +
                    ".\nUsage: #nlp.findTokensRegexPattern(regex:string, text:string-variable)");
        }

        String regex;
        try {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                regex = (String) attributeExpressionExecutors[0].execute(null);
            } else {
                throw new ExecutionPlanCreationException("First parameter should be a constant." +
                        ".\nUsage: #nlp.findTokensRegexPattern(regex:string, text:string-variable)");
            }
        } catch (ClassCastException e) {
            throw new ExecutionPlanCreationException("First parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].getReturnType() +
                    ".\nUsage: #nlp.findTokensRegexPattern(regex:string, text:string-variable)");
        }

        try {
            regexPattern = TokenSequencePattern.compile(regex);
        } catch (Exception e) {
            throw new ExecutionPlanCreationException("Cannot parse given regex " + regex, e);
        }


        if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
            throw new ExecutionPlanCreationException("Second parameter should be a variable." +
                    ".\nUsage: #nlp.findTokensRegexPattern(regex:string, text:string-variable)");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. Regex: %s Stream Parameters: %s", regex,
                    abstractDefinition.getAttributeList()));
        }

        initPipeline();

        ArrayList<Attribute> attributes = new ArrayList<Attribute>(1);

        attributes.add(new Attribute("match", Attribute.Type.STRING));
        attributeCount = regexPattern.getTotalGroups();
        for (int i = 1; i < attributeCount; i++) {
            attributes.add(new Attribute(groupPrefix + i, Attribute.Type.STRING));
        }
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Event received. Regex:%s Event:%s", regexPattern.pattern(), streamEvent));
            }

            Annotation document = pipeline.process(attributeExpressionExecutors[1].execute(streamEvent).toString());

            for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
                TokenSequenceMatcher matcher = regexPattern.getMatcher(sentence.get(CoreAnnotations.TokensAnnotation.class));
                while (matcher.find()) {
                    Object[] data = new Object[attributeCount];
                    data[0] = matcher.group();
                    for (int i = 1; i < attributeCount; i++) {
                        data[i] = matcher.group(i);
                    }
                    StreamEvent newStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    complexEventPopulater.populateComplexEvent(newStreamEvent, data);
                    streamEventChunk.insertBeforeCurrent(newStreamEvent);
                }
            }
            streamEventChunk.remove();
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
