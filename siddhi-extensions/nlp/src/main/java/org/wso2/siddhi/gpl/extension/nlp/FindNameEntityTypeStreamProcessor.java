package org.wso2.siddhi.gpl.extension.nlp;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.StreamEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.gpl.extension.nlp.utility.Constants;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by malithi on 9/3/14.
 */
public class FindNameEntityTypeStreamProcessor extends StreamProcessor {

    private static Logger logger = Logger.getLogger(FindNameEntityTypeStreamProcessor.class);

    //private int[] inStreamParamPosition;
    private Constants.EntityType entityType;
    private boolean groupSuccessiveEntities;
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
        logger.debug("inputs: " + attributeExpressionExecutors.length);

        if (logger.isDebugEnabled()) {
            logger.debug("Initializing Query ...");
        }

        if (attributeExpressionExecutors.length < 3) {
            throw new ExecutionPlanCreationException("Query expects at least three parameters. Received only " + attributeExpressionExecutors
                    .length + ".\nUsage: findNameEntityType(entityType:string, groupSuccessiveEntities:boolean, " +
                    "text:string-variable)");
        }
        String entityTypeParam;
        try {
            entityTypeParam = (attributeExpressionExecutors[0]).execute(null).toString();
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter entityType");
            throw new ExecutionPlanCreationException("First parameter should be of type string. Found " + attributeExpressionExecutors[0].getReturnType() + ".\nUsage: findNameEntityType(entityType:string, " +
                    "groupSuccessiveEntities:boolean, text:string-variable)");
        }

        try {
            this.entityType = Constants.EntityType.valueOf(entityTypeParam.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.error("Entity Type [" + entityTypeParam + "] is not defined", e);
            throw new ExecutionPlanCreationException("First parameter should be one of " + Arrays.deepToString(Constants
                    .EntityType.values()) + ". Found " + entityTypeParam);
        }

        try {
            groupSuccessiveEntities = (Boolean)(attributeExpressionExecutors[1]).execute(null);
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter groupSuccessiveEntities", e);
            throw new ExecutionPlanCreationException("Second parameter should be of type boolean. Found " +
                    attributeExpressionExecutors[1].getReturnType() + ".\nUsage: findNameEntityType(entityType:string, " +
                    "groupSuccessiveEntities:boolean, text:string-variable)");
        }
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. EntityType: %s GroupSuccessiveEntities %s " +
                            "Stream Parameters: %s", entityTypeParam, groupSuccessiveEntities,
                    inputDefinition.getAttributeList()));
        }

        initPipeline();
        ArrayList<Attribute> attributes = new ArrayList<Attribute>(1);
        attributes.add(new Attribute("match", Attribute.Type.STRING));
        return attributes;
    }

    @Override
    protected void process(ComplexEventChunk complexEventChunk, Processor processor, StreamEventCloner streamEventCloner, StreamEventPopulater streamEventPopulater) {
        ComplexEventChunk<StreamEvent> streamEventChunk = complexEventChunk;
        while (complexEventChunk.hasNext()) {
            StreamEvent event = streamEventChunk.next();


            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Event received. Entity Type:%s GroupSuccessiveEntities:%s " +
                        "Event:%s", entityType.name(), groupSuccessiveEntities, event));
            }

            Object[] inStreamData = event.getOutputData();

            Annotation document = new Annotation(attributeExpressionExecutors[2].execute(event).toString());
            pipeline.annotate(document);

            StreamEvent newEvent = null;

            List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

            if (groupSuccessiveEntities) {
                String word;
                String previousWord;
                int previousEventIndex;
                Object[] outStreamData = null;
                boolean added = false;

                for (CoreMap sentence : sentences) {
                    for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                        if (entityType.name().equals(token.get(CoreAnnotations.NamedEntityTagAnnotation.class))) {
                            word = token.get(CoreAnnotations.TextAnnotation.class);
                            if (added) {
                                //previousEventIndex = transformedListEvent.getActiveEvents() - 1;
                                //previousWord = (String) transformedListEvent.getEvent(previousEventIndex).getData0();
                                //transformedListEvent.removeLast();

                                previousWord = newEvent.toString();

                                previousWord = previousWord.concat(" " + word);
                                outStreamData[0] = previousWord;

                                newEvent = streamEventCloner.copyStreamEvent(event);
                                streamEventPopulater.populateStreamEvent(newEvent, outStreamData);
                                streamEventChunk.insertBeforeCurrent(newEvent);
                            } else {

                                outStreamData = new Object[inStreamData.length + 1];
                                outStreamData[0] = word;
                                System.arraycopy(inStreamData, 0, outStreamData, 1, inStreamData.length);
                                //StreamEvent temp = new StreamEvent();
                                //transformedListEvent.add();
                                newEvent = streamEventCloner.copyStreamEvent(event);
                                streamEventPopulater.populateStreamEvent(newEvent, outStreamData);
                                streamEventChunk.insertBeforeCurrent(newEvent);

                            }
                            added = true;
                        } else {
                            added = false;
                        }
                    }
                }
            } else {
                for (CoreMap sentence : sentences) {
                    for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                        if (entityType.name().equals(token.get(CoreAnnotations.NamedEntityTagAnnotation.class))) {
                            String word = token.get(CoreAnnotations.TextAnnotation.class);
                            Object[] outStreamData = new Object[inStreamData.length + 1];
                            outStreamData[0] = word;
                            System.arraycopy(inStreamData, 0, outStreamData, 1, inStreamData.length);
                            newEvent = streamEventCloner.copyStreamEvent(event);
                            streamEventPopulater.populateStreamEvent(newEvent, outStreamData);
                            streamEventChunk.insertBeforeCurrent(newEvent);
                        }
                    }
                }
            }
             streamEventChunk.remove();
        }

        nextProcessor.process(complexEventChunk);
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
