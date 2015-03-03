/*
 * Copyright (c) 2005 - 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.wso2.siddhi.gpl.extension.nlp;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.semgraph.semgrex.SemgrexMatcher;
import edu.stanford.nlp.semgraph.semgrex.SemgrexParseException;
import edu.stanford.nlp.semgraph.semgrex.SemgrexPattern;
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

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SemgrexPatternStreamProcessor extends StreamProcessor {

    private static Logger logger = Logger.getLogger(SemgrexPatternStreamProcessor.class);
    /**
     * represents =<word> pattern
     * used to find named nodes and named relations
     */
    private static final String validationRegex = "(?:\\s*=\\s*)(\\w+)";

    private int attributeCount;
    private int inStreamParamPosition;
    private SemgrexPattern regexPattern;
    private StanfordCoreNLP pipeline;
    private Map<String, Integer> namedElementParamPositions = new HashMap<String, Integer>();

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
                    ".\nUsage: #nlp.findSemgrexPattern(regex:string, text:string-variable)");
        }

        String regex;
        try {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                regex = (String) attributeExpressionExecutors[0].execute(null);
            } else {
                throw new ExecutionPlanCreationException("First parameter should be a constant." +
                        ".\nUsage: #nlp.findSemgrexPattern(regex:string, text:string-variable)");
            }
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter regex", e);
            throw new ExecutionPlanCreationException("First parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].getReturnType() +
                    ".\nUsage: #nlp.findSemgrexPattern(regex:string, text:string-variable)");
        }

        try {
            regexPattern = SemgrexPattern.compile(regex);
        } catch (SemgrexParseException e) {
            logger.error("Error in parsing semgrex pattern", e);
            throw new ExecutionPlanCreationException("Cannot parse given regex: " + regex, e);
        }


        if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
            throw new ExecutionPlanCreationException("Second parameter should be a variable." +
                    ".\nUsage: #nlp.findSemgrexPattern(regex:string, text:string-variable)");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. Regex: %s Stream Parameters: %s", regex,
                    abstractDefinition.getAttributeList()));
        }

        initPipeline();

        ArrayList<Attribute> attributes = new ArrayList<Attribute>(1);
        attributes.add(new Attribute("match", Attribute.Type.STRING));


        // Find all named elements in the regular expression and add them to the output stream definition attributes
        Set<String> namedElementSet = new HashSet<String>();
        Pattern validationPattern = Pattern.compile(validationRegex);
        Matcher validationMatcher = validationPattern.matcher(regex);
        while (validationMatcher.find()) {
            //group 1 of the matcher gives the node name or the relation name
            namedElementSet.add(validationMatcher.group(1).trim());
        }

        attributeCount = 1;
        for (String namedElement : namedElementSet) {
            attributes.add(new Attribute(namedElement, Attribute.Type.STRING));
            namedElementParamPositions.put(namedElement, attributeCount++);
        }
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();

            Annotation document = pipeline.process(attributeExpressionExecutors[1].execute(streamEvent).toString());

            for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
                SemanticGraph graph = sentence.get(SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
                SemgrexMatcher matcher = regexPattern.matcher(graph);

                while (matcher.find()) {
                    Object[] data = new Object[attributeCount];
                    data[0] = matcher.getMatch().value();

                    for (String nodeName : matcher.getNodeNames()) {
                        if (namedElementParamPositions.containsKey(nodeName)) {
                            data[namedElementParamPositions.get(nodeName)] = matcher.getNode(nodeName) == null
                                    ? null : matcher.getNode(nodeName).word();
                        }
                    }

                    for (String relationName : matcher.getRelationNames()) {
                        if (namedElementParamPositions.containsKey(relationName)) {
                            data[namedElementParamPositions.get(relationName)] = matcher.getRelnString(relationName);
                        }
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
