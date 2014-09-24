package org.wso2.siddhi.gpl.extension.nlp;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
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
import org.wso2.siddhi.query.api.expression.constant.BoolConstant;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by malithi on 9/3/14.
 */
@SiddhiExtension(namespace = "nlp", function = "findNameEntityType")
public class NameEntityTypeTransformProcessor extends TransformProcessor {

    private static Logger logger = Logger.getLogger(NameEntityTypeTransformProcessor.class);

    private int inStreamParamPosition;
    private Constants.EntityType entityType;
    private boolean groupSuccessiveEntities;
    private StanfordCoreNLP pipeline;

    @Override
    protected void init(Expression[] expressions, List<ExpressionExecutor> expressionExecutors, StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition, String elementId, SiddhiContext siddhiContext) {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing Query ...");
        }

        for (Expression expression:expressions){
            System.out.println(expression);
        }

        if (expressions.length < 3){
            throw new QueryCreationException("Query expects at least three parameters. Received only " + expressions
                    .length + ".\nUsage: findNameEntityType(entityType:string, groupSuccessiveEntities:boolean, " +
                    "text:string-variable)");
        }

        String entityTypeParam;
        try {
            entityTypeParam = ((StringConstant)expressions[0]).getValue();
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter entityType");
            throw new QueryCreationException("First parameter should be of type string. Found " + Constants
                    .getType(expressions[0]) + ".\nUsage: findNameEntityType(entityType:string, " +
                    "groupSuccessiveEntities:boolean, text:string-variable)");
        }

        try {
            this.entityType = Constants.EntityType.valueOf(entityTypeParam.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.error("Entity Type ["+ entityTypeParam + "] is not defined",e);
            throw new QueryCreationException("First parameter should be one of " + Arrays.deepToString(Constants
                    .EntityType.values()) + ". Found " + entityTypeParam);
        }

        try {
            groupSuccessiveEntities = ((BoolConstant)expressions[1]).getValue();
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter groupSuccessiveEntities",e);
            throw new QueryCreationException("Second parameter should be of type boolean. Found " + Constants.getType
                    (expressions[1]) + ".\nUsage: findNameEntityType(entityType:string, " +
                    "groupSuccessiveEntities:boolean, text:string-variable)");
        }

        if (expressions[2] instanceof Variable){
            inStreamParamPosition = inStreamDefinition.getAttributePosition(((Variable)expressions[2])
                    .getAttributeName());
        }else{
            throw new QueryCreationException("Third parameter should be a variable. Found " + Constants.getType
                    (expressions[2]) + ".\nUsage: findNameEntityType(entityType:string, " +
                    "groupSuccessiveEntities:boolean, text:string-variable)");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. EntityType: %s GroupSuccessiveEntities %s " +
                            "Stream Parameters: %s", entityTypeParam, groupSuccessiveEntities,
                    inStreamDefinition.getAttributeList()));
        }

        initPipeline();

        if (outStreamDefinition == null) {
            this.outStreamDefinition = new StreamDefinition().name("nameEntityTypeMatchStream");

            this.outStreamDefinition.attribute("match", Attribute.Type.STRING);

            for(Attribute strDef : inStreamDefinition.getAttributeList()) {
                this.outStreamDefinition.attribute(strDef.getName(), strDef.getType());
            }
        }
    }

    @Override
    protected InStream processEvent(InEvent inEvent) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Event received. Entity Type:%s GroupSuccessiveEntities:%s " +
                            "Event:%s", entityType.name(), groupSuccessiveEntities, inEvent));
        }

        Object [] inStreamData = inEvent.getData();

        Annotation document = new Annotation((String)inEvent.getData(inStreamParamPosition));
        pipeline.annotate(document);

        InListEvent transformedListEvent = new InListEvent();

        List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

        if (groupSuccessiveEntities){
            int previousCount = 0;
            int count = 0;
            int previousWordIndex;
            String word;

            for (CoreMap sentence : sentences) {
                for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                    word = token.get(CoreAnnotations.TextAnnotation.class);
                    if (entityType.name().equals(token.get(CoreAnnotations.NamedEntityTagAnnotation.class))) {
                        count++;
                        if (previousCount != 0 && (previousCount + 1) == count) {
                            previousWordIndex = transformedListEvent.getActiveEvents() - 1;
                            String previousWord = (String)transformedListEvent.getEvent(previousWordIndex).getData0();
                            transformedListEvent.removeLast();

                            previousWord = previousWord.concat(" " + word);
                            Object [] outStreamData = new Object[inStreamData.length + 1];
                            outStreamData[0] = previousWord;
                            System.arraycopy(inStreamData, 0, outStreamData, 1, inStreamData.length);
                            transformedListEvent.addEvent(new InEvent(inEvent.getStreamId(), System.currentTimeMillis(),
                                    outStreamData));
                        } else {
                            Object [] outStreamData = new Object[inStreamData.length + 1];
                            outStreamData[0] = word;
                            System.arraycopy(inStreamData, 0, outStreamData, 1, inStreamData.length);
                            transformedListEvent.addEvent(new InEvent(inEvent.getStreamId(), System.currentTimeMillis(),
                                    outStreamData));
                        }
                        previousCount = count;
                    } else {
                        count = 0;
                        previousCount = 0;
                    }
                }
            }
        }else {
            for (CoreMap sentence : sentences) {
                for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
                    String word = token.get(CoreAnnotations.TextAnnotation.class);
                    if (entityType.name().equals(token.get(CoreAnnotations.NamedEntityTagAnnotation.class))) {
                        Object [] outStreamData = new Object[inStreamData.length + 1];
                        outStreamData[0] = word;
                        System.arraycopy(inStreamData, 0, outStreamData, 1, inStreamData.length);
                        transformedListEvent.addEvent(new InEvent(inEvent.getStreamId(), System.currentTimeMillis(),
                                outStreamData));
                    }
                }
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
