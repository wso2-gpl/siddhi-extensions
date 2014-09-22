package org.wso2.siddhi.gpl.extension.nlp;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
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
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.AttributeNotExistException;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.List;
import java.util.Properties;

/**
 * Created by malithi on 9/3/14.
 */

@SiddhiExtension(namespace = "nlp", function = "findTokensRegexPattern")
public class TokensRegexPatternTransformProcessor extends TransformProcessor{

    private static Logger logger = Logger.getLogger(TokensRegexPatternTransformProcessor.class);
    private static final String groupPrefix = "group_";

    private int attributeCount;
    private int inStreamParamPosition;
    private TokenSequencePattern regexPattern;
    private StanfordCoreNLP pipeline;

    @Override
    protected void init(Expression[] expressions, List<ExpressionExecutor> expressionExecutors, StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition, String elementId, SiddhiContext siddhiContext) {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing Query ...");
        }

        if (expressions.length < 2){
            throw new QueryCreationException("Query expects at least two parameters. Usage: findTokensRegexPattern" +
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
            regexPattern = TokenSequencePattern.compile(regex);
        } catch (Exception e) {
            logger.error("Error in parsing token regex pattern",e);
            throw new QueryCreationException("Cannot parse given regex");
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
            this.outStreamDefinition = new StreamDefinition().name("tokensRegexPatternMatchStream");

            this.outStreamDefinition.attribute("match", Attribute.Type.STRING);

            attributeCount = regexPattern.getTotalGroups();
            for (int i = 1; i < regexPattern.getTotalGroups(); i++){
                this.outStreamDefinition.attribute(groupPrefix + i, Attribute.Type.STRING);
            }

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

        for (CoreMap sentence:document.get(CoreAnnotations.SentencesAnnotation.class)){
            TokenSequenceMatcher matcher = regexPattern.getMatcher(sentence.get(CoreAnnotations.TokensAnnotation.class));

            while( matcher.find()){
                Object [] outStreamData = new Object[inStreamData.length + attributeCount];
                outStreamData[0] = matcher.group();

                int position;
                for (int i = 1; i < (matcher.groupCount() + 1);i++){
                    try {
                        position = this.outStreamDefinition.getAttributePosition(groupPrefix + i);
                        outStreamData[position] = matcher.group(i);
                    } catch (AttributeNotExistException e) {
                        //exception ignored
                    }
                }

                System.arraycopy(inStreamData, 0, outStreamData, attributeCount, inStreamData.length);
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
