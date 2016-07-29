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

public class RelationshipByVerbStreamProcessor extends StreamProcessor {

    private static Logger logger = Logger.getLogger(RelationshipByVerbStreamProcessor.class);

    /**
     * Used to find subject, object and verb, where subject is optional
     */
    private static final String verbOptSub = "{lemma:%s}=verb ?>/nsubj|agent|xsubj/ {}=subject " +
            ">/dobj|iobj|nsubjpass/ {}=object";
    /**
     * Used to find subject, object and verb, where object is optional
     */
    private static final String verbOptObj = "{lemma:%s}=verb >/nsubj|agent|xsubj/ {}=subject " +
            "?>/dobj|iobj|nsubjpass/ {}=object";

    private SemgrexPattern verbOptSubPattern;
    private SemgrexPattern verbOptObjPattern;
    private String verb;
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
                    ".\nUsage: #nlp.findRelationshipByVerb(verb:string, text:string-variable)");
        }

        String verb;
        try {
            if (attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor) {
                verb = (String) attributeExpressionExecutors[0].execute(null);
            } else {
                throw new ExecutionPlanCreationException("First parameter should be a constant." +
                        ".\nUsage: #nlp.findRelationshipByVerb(verb:string, text:string-variable)");
            }
        } catch (ClassCastException e) {
            throw new ExecutionPlanCreationException("First parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].getReturnType() +
                    ".\nUsage: #nlp.findRelationshipByVerb(verb:string, text:string-variable)");
        }

        try {
            verbOptSubPattern = SemgrexPattern.compile(String.format(verbOptSub,verb));
            verbOptObjPattern = SemgrexPattern.compile(String.format(verbOptObj,verb));
        } catch (SemgrexParseException e) {
            throw new ExecutionPlanCreationException("First parameter is not a verb. Found " + verb +
                    "\nUsage: #nlp.findRelationshipByVerb(verb:string, text:string-variable)");
        }

        if (!(attributeExpressionExecutors[1] instanceof VariableExpressionExecutor)) {
            throw new ExecutionPlanCreationException("Second parameter should be a variable." +
                    ".\nUsage: #nlp.findRelationshipByVerb(verb:string, text:string-variable)");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. verb: %s Stream Parameters: %s", verb,
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
                    logger.debug(String.format("Event received. Verb:%s Event:%s", verb, streamEvent));
                }

                Annotation document = pipeline.process(attributeExpressionExecutors[1].execute(streamEvent).toString());

                Set<Event> eventSet = new HashSet<Event>();

                for (CoreMap sentence : document.get(CoreAnnotations.SentencesAnnotation.class)) {
                    findMatchingEvents(sentence, verbOptSubPattern, eventSet);
                    findMatchingEvents(sentence, verbOptObjPattern, eventSet);
                }

                for (Event event : eventSet) {
                    Object[] data = new Object[3];
                    data[0] = event.subject;
                    data[1] = event.object;
                    data[2] = event.verb;
                    StreamEvent newStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    complexEventPopulater.populateComplexEvent(newStreamEvent, data);
                    streamEventChunk.insertBeforeCurrent(newStreamEvent);
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

    private static final class Event{
        String subject;
        String object;
        String verb;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Event event = (Event) o;

            if (object != null ? !object.equals(event.object) : event.object != null) {
                return false;
            }

            if (subject != null ? !subject.equals(event.subject) : event.subject != null) {
                return false;
            }

            return verb.equals(event.verb);
        }

        @Override
        public int hashCode() {
            int result = subject != null ? subject.hashCode() : 0;
            result = 31 * result + (object != null ? object.hashCode() : 0);
            result = 31 * result + (verb != null ? verb.hashCode() : 0);
            return result;
        }
    }
    
    private void findMatchingEvents(CoreMap sentence, SemgrexPattern pattern, Set<Event> eventSet){
        SemanticGraph graph = sentence.get(SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
        SemgrexMatcher matcher = pattern.matcher(graph);

        while(matcher.find()){
            Event event = new Event();
            event.verb = matcher.getNode(Constants.verb) == null ? null : matcher.getNode(Constants.verb).word();
            event.subject = matcher.getNode(Constants.subject) == null ? null : matcher.getNode(Constants.subject).word();
            event.object = matcher.getNode(Constants.object) == null ? null : matcher.getNode(Constants.object).word();

            eventSet.add(event);
        }
    }
}
