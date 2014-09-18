/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
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
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.ListEvent;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.gpl.extension.nlp.utility.Constants;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Created by malithi on 9/3/14.
 */
@SiddhiExtension(namespace = "nlp", function = "findRelationshipByVerb")
public class RelationshipByVerbTransformProcessor extends TransformProcessor {

    private static Logger logger = Logger.getLogger(RelationshipByVerbTransformProcessor.class);

    private static final String regexOptSub = "{lemma:%s}=verb ?>/nsubj|agent|xsubj/ {}=subject " +
            ">/dobj|iobj|nsubjpass/ {}=object";
    private static final String regexOptObj = "{lemma:%s}=verb >/nsubj|agent|xsubj/ {}=subject " +
            "?>/dobj|iobj|nsubjpass/ {}=object";

    private int inStreamParamPosition;
    private SemgrexPattern regexOptSubPattern;
    private SemgrexPattern regexOptObjPattern;
    private String verb;
    private StanfordCoreNLP pipeline;

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
            if (!verb.equals(event.verb)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = subject != null ? subject.hashCode() : 0;
            result = 31 * result + (object != null ? object.hashCode() : 0);
            result = 31 * result + verb.hashCode();
            return result;
        }
    }


    @Override
    protected void init(Expression[] expressions, List<ExpressionExecutor> expressionExecutors, StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition, String elementId, SiddhiContext siddhiContext) {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing Query ...");
        }

        if (expressions.length < 2){
            throw new QueryCreationException("Query expects at least two parameters. Usage: findRelationshipByVerb" +
                    "(verb:string, text:string)");
        }

        try {
            verb = ((StringConstant)expressions[0]).getValue();
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter verb",e);
            throw new QueryCreationException("Parameter verb should be of type string");
        }

        try {
            regexOptSubPattern = SemgrexPattern.compile(String.format(regexOptSub,verb));
            regexOptObjPattern = SemgrexPattern.compile(String.format(regexOptObj,verb));
        } catch (SemgrexParseException e) {
            logger.error("Error in initializing relation extracting pattern for verb",e);
            throw new QueryCreationException("Parameter verb is invalid");
        }

        if (expressions[1] instanceof Variable){
            inStreamParamPosition = inStreamDefinition.getAttributePosition(((Variable)expressions[1])
                    .getAttributeName());
        }else{
            throw new QueryCreationException("Second parameter should be a variable");
        }


        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. Verb: %s Stream Parameters: %s", verb,
                    inStreamDefinition.getAttributeList()));
        }

        initPipeline();

        if (outStreamDefinition == null) {
            this.outStreamDefinition = new StreamDefinition().name("relationshipByVerbMatchStream");

            this.outStreamDefinition.attribute(Constants.subject, Attribute.Type.STRING);
            this.outStreamDefinition.attribute(Constants.object, Attribute.Type.STRING);
            this.outStreamDefinition.attribute(Constants.verb, Attribute.Type.STRING);

            for(Attribute strDef : inStreamDefinition.getAttributeList()) {
                this.outStreamDefinition.attribute(strDef.getName(), strDef.getType());
            }
        }
    }

    @Override
    protected InStream processEvent(InEvent inEvent) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Event received. Verb:%s Event:%s", verb, inEvent));
        }

        Object [] inStreamData = inEvent.getData();

        Annotation document = pipeline.process((String)inEvent.getData(inStreamParamPosition));

        InListEvent transformedListEvent = new InListEvent();

        Set<Event> eventSet = new HashSet<Event>();

        for (CoreMap sentence:document.get(CoreAnnotations.SentencesAnnotation.class)){
            findMatchingEvents(sentence, regexOptSubPattern, eventSet);
            findMatchingEvents(sentence, regexOptObjPattern, eventSet);
        }

        for (Event event: eventSet){
            Object [] outStreamData = new Object[inStreamData.length + 3];
            outStreamData[0] = event.subject;
            outStreamData[1] = event.object;
            outStreamData[2] = event.verb;
            System.arraycopy(inStreamData, 0, outStreamData, 3, inStreamData.length);
            transformedListEvent.addEvent(new InEvent(inEvent.getStreamId(), System.currentTimeMillis(),
                    outStreamData));
        }

        return transformedListEvent;
    }

    @Override
    protected InStream processEvent(InListEvent inListEvent) {
        InListEvent transformedListEvent = new InListEvent();
        for (org.wso2.siddhi.core.event.Event event : inListEvent.getEvents()) {
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

    private void initPipeline(){
        logger.info("Initializing Annotator pipeline ...");
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse");

        pipeline = new StanfordCoreNLP(props);
        logger.info("Annotator pipeline initialized");
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
