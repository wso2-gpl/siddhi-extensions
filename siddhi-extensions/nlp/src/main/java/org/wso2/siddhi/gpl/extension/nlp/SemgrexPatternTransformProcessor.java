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
import org.wso2.siddhi.query.api.exception.AttributeNotExistException;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.StringConstant;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by malithi on 9/3/14.
 */
@SiddhiExtension(namespace = "nlp", function = "findSemgrexPattern")
public class SemgrexPatternTransformProcessor extends TransformProcessor {

    private static Logger logger = Logger.getLogger(SemgrexPatternTransformProcessor.class);
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

    @Override
    protected void init(Expression[] expressions, List<ExpressionExecutor> expressionExecutors, StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition, String elementId, SiddhiContext siddhiContext) {
        if (logger.isDebugEnabled()) {
            logger.debug("Initializing Query ...");
        }

        if (expressions.length < 2){
            throw new QueryCreationException("Query expects at least two parameters. Received only " + expressions
                    .length + ".\n" +
                    "Usage: findSemgrexPattern(regex:string, text:string)");
        }

        String regex;
        try {
            regex = ((StringConstant)expressions[0]).getValue();
        } catch (ClassCastException e) {
            logger.error("Error in reading parameter regex",e);
            throw new QueryCreationException("First parameter should be of type string. Found " + Constants.getType
                    (expressions[0]) + ".\n" +
                    "Usage: findSemgrexPattern(regex:string, text:string)");
        }

        try {
            regexPattern = SemgrexPattern.compile(regex);
        } catch (SemgrexParseException e) {
            logger.error("Error in parsing semgrex pattern",e);
            throw new QueryCreationException("Cannot parse given regex " + regex + " Error: [" + e.getMessage() + "]");
        }

        if (expressions[1] instanceof Variable){
            inStreamParamPosition = inStreamDefinition.getAttributePosition(((Variable)expressions[1])
                    .getAttributeName());
        }else{
            throw new QueryCreationException("Second parameter should be a variable. Found " + Constants.getType
                    (expressions[1]) + ".\n" +
                    "Usage: findSemgrexPattern(regex:string, text:string)");
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Query parameters initialized. Regex: %s Stream Parameters: %s", regex,
                    inStreamDefinition.getAttributeList()));
        }

        initPipeline();

        if (outStreamDefinition == null) {

            this.outStreamDefinition = new StreamDefinition().name("semgrexPatternMatchStream");

            this.outStreamDefinition.attribute("match", Attribute.Type.STRING);

            // Find all named elements in the regular expression and add them to the output stream definition attributes
            Set<String> namedElementSet = new HashSet<String>();
            Pattern validationPattern = Pattern.compile(validationRegex);
            Matcher validationMatcher = validationPattern.matcher(regex);
            while (validationMatcher.find()){
                //group 1 of the matcher gives the node name or the relation name
                namedElementSet.add(validationMatcher.group(1).trim());
            }

            attributeCount = 1;
            for (String namedElement:namedElementSet){
                this.outStreamDefinition.attribute(namedElement, Attribute.Type.STRING);
                namedElementParamPositions.put(namedElement,attributeCount);
                attributeCount++;
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
            SemanticGraph graph = sentence.get(SemanticGraphCoreAnnotations.CollapsedCCProcessedDependenciesAnnotation.class);
            SemgrexMatcher matcher = regexPattern.matcher(graph);

            while(matcher.find()){
                Object [] outStreamData = new Object[inStreamData.length + attributeCount];
                outStreamData[0] = matcher.getMatch().value();

                for(String nodeName:matcher.getNodeNames()){
                    if (namedElementParamPositions.containsKey(nodeName)){
                        outStreamData[namedElementParamPositions.get(nodeName)] = matcher.getNode(nodeName) == null
                                ? null : matcher.getNode(nodeName)
                                .word();
                    }
                }

                for(String relationName:matcher.getRelationNames()){
                    if (namedElementParamPositions.containsKey(relationName)){
                        outStreamData[namedElementParamPositions.get(relationName)] = matcher.getRelnString(relationName);
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
