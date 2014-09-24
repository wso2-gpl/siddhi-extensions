package org.wso2.siddhi.gpl.extension.nlp;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RelationshipByRegexTransformProcessorTestCase extends NlpTransformProcessorTestCase {
    private static Logger logger = Logger.getLogger(RelationshipByRegexTransformProcessorTestCase.class);
    private static List<String[]> data;

    @BeforeClass
    public static void loadData() throws Exception {
        data = new ArrayList<String[]>();

        data.add(new String[]{"Professeur Jamelski",
                "Bill Gates donates $31million to fight Ebola"});
        data.add(new String[]{"going info",
                "Trail Blazers owner Paul Allen donates $9million to fight Ebola"});
        data.add(new String[]{"WinBuzzer",
                "Microsoft co-founder Paul Allen donates $9million to help fight Ebola in Africa"});
        data.add(new String[]{"theSun",
                "Malaysia to donate 20.9m medical gloves to five Ebola-hit countries"});
        data.add(new String[]{"CIDI",
                "@gatesfoundation donates $50M in support of #Ebola relief. Keep up the #SmartCompassion, " +
                        "@BillGates & @MelindaGates!"});
        data.add(new String[]{"African Farm Network",
                "Ellen Donates 100 Bags of Rice, U.S.$5,000 to JFK[Inquirer]In the wake of the deadly Ebola virus"});
    }

    @Override
    public void setUpChild() {
        siddhiManager.defineStream("define stream  RelationshipByRegexIn(username string, text string )");
    }

    @Test
    public void testRelationshipByRegex() throws Exception{
        //expecting subjects
        String[] expectedSubjects = {"Gates" , "Allen", "Allen", "Malaysia" , "@gatesfoundation", "Ellen"};
        //expecting objects
        String[] expectedObjects = {"31million" , "9million", "9million", "20.9" , "50M", "Bags"};
        //expecting verbs
        String[] expectedVerbs = {"donates" , "donates", "donates", "donate" , "donates", "Donates"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {0,1,2,3,4,5};

        List<Event> outputEvents = testRelationshipByRegex("({} </xcomp|dobj/ ({lemma:donate}=verb >nsubj {}=subject)" +
                ") >/nsubj|num/ {}=object");

        for (int i = 0; i < outputEvents.size(); i++){
            Event event = outputEvents.get(i);
            //Compare expected subject and received subject
            assertEquals(expectedSubjects[i], event.getData(0));
            //Compare expected object and received object
            assertEquals(expectedObjects[i], event.getData(1));
            //Compare expected verb and received verb
            assertEquals(expectedVerbs[i], event.getData(2));
            //Compare expected output stream username and received username
            assertEquals(data.get(matchedInStreamIndices[i])[0], event.getData(3));
            //Compare expected output stream text and received text
            assertEquals(data.get(matchedInStreamIndices[i])[1], event.getData(4));
        }
    }

    @Test(expected = org.wso2.siddhi.core.exception.QueryCreationException.class)
    public void testQueryCreationExceptionInvalidNoOfParams() {
        logger.info("Test: QueryCreationException at Invalid No Of Params");
        siddhiManager.addQuery("from RelationshipByRegexIn#transform.nlp:findRelationshipByRegex" +
                "        (text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionRegexNotContainVerb(){
        logger.info("Test: QueryCreationException at Regex does not contain Verb");
        siddhiManager.addQuery("from RelationshipByRegexIn#transform.nlp:findRelationshipByRegex" +
                "        ( '{} >/nsubj|agent/ {}=subject ?>/dobj/ {}=object',text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionRegexNotContainSubject(){
        logger.info("Test: QueryCreationException at Regex does not contain Subject");
        siddhiManager.addQuery("from RelationshipByRegexIn#transform.nlp:findRelationshipByRegex" +
                "        ('{}=verb >/nsubj|agent/ {} ?>/dobj/ {}=object',text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionRegexNotContainObject(){
        logger.info("Test: QueryCreationException at Regex does not contain Object");
        siddhiManager.addQuery("from RelationshipByRegexIn#transform.nlp:findRelationshipByRegex" +
                "        ('{}=verb >/nsubj|agent/ {}=subject ?>/dobj/ {}',text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionCannotParseRegex(){
        logger.info("Test: QueryCreationException at Regex does not contain Object");
        siddhiManager.addQuery("from RelationshipByRegexIn#transform.nlp:findRelationshipByRegex" +
                "        ('{}=verb ??>/nsubj|agent/ {}=subject ?>/dobj/ {}',text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionRegexTypeMismatch(){
        logger.info("Test: QueryCreationException at Regex parsing");
        siddhiManager.addQuery("from RelationshipByRegexIn#transform.nlp:findRelationshipByRegex" +
                "        ('{}=verb >/nsubj|agent/ {}=subject ?>/dobj/ {}',text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n");
    }


    private List<Event> testRelationshipByRegex(String regex) throws Exception{
        logger.info(String.format("Test: Regex = %s",regex
        ));
        String query = "from RelationshipByRegexIn#transform.nlp:findRelationshipByRegex" +
                "        ( '%s', text ) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n";
        start = System.currentTimeMillis();
        String queryReference = siddhiManager.addQuery(String.format(query,regex));
        end = System.currentTimeMillis();

        logger.info(String.format("Time to add query: [%f sec]", ((end - start)/1000f)));

        final List<Event> eventList = new ArrayList<Event>();

        siddhiManager.addCallback(queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event:inEvents){
                    Collections.addAll(eventList, event.toArray());
                }
            }
        });

        generateEvents();

        return eventList;
    }

    private void generateEvents() throws Exception{
        InputHandler inputHandler = siddhiManager.getInputHandler("RelationshipByRegexIn");
        for(String[] dataLine:data) {
            inputHandler.send(new Object[]{dataLine[0], dataLine[1]});
        }
    }

}