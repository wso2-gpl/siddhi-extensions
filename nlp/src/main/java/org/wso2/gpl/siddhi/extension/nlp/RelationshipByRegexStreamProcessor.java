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
package org.wso2.gpl.siddhi.extension.nlp;

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
import org.wso2.gpl.siddhi.extension.nlp.utility.Constants;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RelationshipByRegexStreamProcessor extends StreamProcessor {

    private static Logger logger = Logger.getLogger(RelationshipByRegexStreamProcessor.class);

    /**
     * represents {}=<word> pattern
     * used to find named nodes
     */
    private static final String validationRegex = "(?:[{.*}]\\s*=\\s*)(\\w+)";

    private SemgrexPattern regexPattern;
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
                    attributeExpressionLength + ".\nUsage: #nlp.findRelationshipByRegex(regex:string, text:string-variable)");
        }

        String regex;
        try {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                regex = (String) attributeExpressionExecutors[0].execute(null);
            } else {
                throw new ExecutionPlanCreationException("First parameter should be a constant." +
                        ".\nUsage: #nlp.findRelationshipByRegex(regex:string, text:string-variable)");
            }
        } catch (ClassCastException e) {
            throw new ExecutionPlanCreationException("First parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].getReturnType() +
                    ".\nUsage: #nlp.findRelationshipByRegex(regex:string, text:string-variable)");
        }

        try {
            regexPattern = SemgrexPattern.compile(regex);
        } catch (SemgrexParseException e) {
            throw new ExecutionPlanCreationException("Cannot parse given regex: " + regex, e);
        }

        Set<String> namedNodeSet = new HashSet<String>();
        Pattern validationPattern = Pattern.compile(validationRegex);
        Matcher validationMatcher = validationPattern.matcher(regex);
        while (validationMatcher.find()) {
            //group 1 of the matcher gives the node name
            namedNodeSet.add(validationMatcher.group(1).trim());
        }

        if (!namedNodeSet.contains(Constants.subject)) {
            throw new ExecutionPlanCreationException("Given regex " + regex + " does not contain a named node as subject. " +
                    "Expect a node named as {}=subject");
        }

        if (!namedNodeSet.contains(Constants.object)) {
            throw new ExecutionPlanCreationException("Given regex " + regex + " does not contain a named node as object. " +
                    "Expect a node named as {}=object");
        }

        if (!namedNodeSet.contains(Constants.verb)) {
            throw new ExecutionPlanCreationException("Given regex " + regex + " does not contain a named node as verb. Expect" +
                    " a node named as {}=verb");
        }

        if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
            throw new ExecutionPlanCreationException("Second parameter should be a variable." +
                    ".\nUsage: #nlp.findRelationshipByRegex(regex:string, text:string-variable)");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. Regex: %s Stream Parameters: %s", regex,
                    abstractDefinition.getAttributeList()));
        }

        initPipeline();
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(1);
        attributes.add(new Attribute(Constants.subject, Attribute.Type.STRING));
        attributes.add(new Attribute(Constants.object, Attribute.Type.STRING));
        attributes.add(new Attribute(Constants.verb, Attribute.Type.STRING));
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("Event received. Regex:%s Event:%s", regexPattern.pattern(), streamEvent));
                }

                Annotation document = pipeline.process(attributeExpressionExecutors[1].execute(streamEvent).toString());

                SemgrexMatcher matcher;
                SemanticGraph graph;
                for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
                    graph = sentence.get(SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
                    matcher = regexPattern.matcher(graph);

                    while (matcher.find()) {
                        Object[] data = new Object[3];
                        data[0] = matcher.getNode(Constants.subject) == null ? null : matcher.getNode(Constants.subject)
                                .word();
                        data[1] = matcher.getNode(Constants.object) == null ? null : matcher.getNode(Constants.object)
                                .word();
                        data[2] = matcher.getNode(Constants.verb) == null ? null : matcher.getNode(Constants.verb)
                                .word();
                        StreamEvent newStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                        complexEventPopulater.populateComplexEvent(newStreamEvent, data);
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
