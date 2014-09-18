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
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by malithi on 9/3/14.
 */
@SiddhiExtension(namespace = "nlp", function = "findRelationshipByRegex")
public class RelationshipByRegexTransformProcessor extends TransformProcessor {

    private static Logger logger = Logger.getLogger(RelationshipByRegexTransformProcessor.class);
    private static final String validationRegex = "(?:[{.*}]\\s*=\\s*)(\\w+)";

    private int inStreamParamPosition;
    private SemgrexPattern regexPattern;
    private StanfordCoreNLP pipeline;

    @Override
    protected void init(Expression[] expressions, List<ExpressionExecutor> expressionExecutors, StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition, String elementId, SiddhiContext siddhiContext) {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing Query ...");
        }

        if (expressions.length < 2){
            throw new QueryCreationException("Query expects at least two parameters. Usage: findRelationshipByRegex" +
                    "(regex:string, text:string)");
        }

        String regex;
        try {
            regex = ((StringConstant)expressions[0]).getValue();
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter regex",e);
            throw new QueryCreationException("Parameter regex should be of type string");
        }

        try {
            regexPattern = SemgrexPattern.compile(regex);
        } catch (SemgrexParseException e) {
            logger.error("Error in parsing semgrex pattern",e);
            throw new QueryCreationException("Cannot parse given regex");
        }

        Set<String> namedNodeSet = new HashSet<String>();
        Pattern validationPattern = Pattern.compile(validationRegex);
        Matcher validationMatcher = validationPattern.matcher(regex);
        while (validationMatcher.find()){
            namedNodeSet.add(validationMatcher.group(1).trim());
        }

        if (!namedNodeSet.contains(Constants.subject)){
            throw new QueryCreationException("Regex should contain a named node as subject");
        }

        if (!namedNodeSet.contains(Constants.object)){
            throw new QueryCreationException("Regex should contain a named node as object");
        }

        if (!namedNodeSet.contains(Constants.verb)){
            throw new QueryCreationException("Regex should contain a named node as verb");
        }

        if (expressions[1] instanceof Variable){
            inStreamParamPosition = inStreamDefinition.getAttributePosition(((Variable)expressions[1])
                    .getAttributeName());
        }else{
            throw new QueryCreationException("Second parameter should be a variable");
        }


        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. Regex: %s Stream Parameters: %s", regex,
                    inStreamDefinition.getAttributeList()));
        }

        initPipeline();

        if (outStreamDefinition == null) {
            this.outStreamDefinition = new StreamDefinition().name("relationshipByRegexMatchStream");

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
            logger.debug(String.format("Event received. Regex:%s Event:%s", regexPattern.pattern(), inEvent));
        }

        Object [] inStreamData = inEvent.getData();

        Annotation document = pipeline.process((String)inEvent.getData(inStreamParamPosition));

        InListEvent transformedListEvent = new InListEvent();

        SemgrexMatcher matcher;
        SemanticGraph graph;
        for (CoreMap sentence:document.get(CoreAnnotations.SentencesAnnotation.class)){
            graph = sentence.get(SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
            matcher = regexPattern.matcher(graph);

            while(matcher.find()){
                Object [] outStreamData = new Object[inStreamData.length + 3];
                outStreamData[0] = matcher.getNode(Constants.subject) == null ? null : matcher.getNode(Constants.subject)
                        .word();
                outStreamData[1] = matcher.getNode(Constants.object) == null  ? null : matcher.getNode(Constants.object)
                        .word();
                outStreamData[2] = matcher.getNode(Constants.verb) == null  ? null : matcher.getNode(Constants.verb)
                        .word();
                System.arraycopy(inStreamData, 0, outStreamData, 3, inStreamData.length);
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

    private void initPipeline(){
        logger.info("Initializing Annotator pipeline ...");
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse");

        pipeline = new StanfordCoreNLP(props);
        logger.info("Annotator pipeline initialized");
    }
}
