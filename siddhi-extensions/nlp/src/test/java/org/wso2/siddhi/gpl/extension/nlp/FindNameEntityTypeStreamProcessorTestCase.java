package org.wso2.siddhi.gpl.extension.nlp;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FindNameEntityTypeStreamProcessorTestCase extends NlpTransformProcessorTestCase {
    private static Logger logger = Logger.getLogger(FindNameEntityTypeStreamProcessorTestCase.class);
    private static List<String[]> data;
    private String defineStream = "@config(async = 'true') define stream NameEntityTypeIn (username string, " +
            "text string ); \n";

    @BeforeClass
    public static void loadData(){
        data = new ArrayList<String[]>();

        data.add(new String[]{"Onasis Elom Gaisie",
                "RT @BBCAfrica: #Ghana President John Mahama, the chairman of regional body Ecowas, " +
                        "is on a one day tour of three West African countries wra…"});
        data.add(new String[]{"Surviving Jim",
                "RT @SurvivalBasic: Woman ARRIVING in West Africa From Morocco Tests Positive For Ebola: By Lizzie Bennett A South... …"});
        data.add(new String[]{"Atlanta News",
                "Obama to visit Atlanta Tuesday for Ebola update: President Barack Obama is scheduled Tuesday to...  #Atlanta #GA"});
        data.add(new String[]{"Michael Chavez",
                "Malaysia to send 20 million medical gloves to fight Ebola: Malaysia will send more than 20 million medical rub..."});
        data.add(new String[]{"Professeur Jamelski",
                "RT @DailyMirror: Bill Gates donates £31million to fight Ebola"});
        data.add(new String[]{"ethioembabidjan",
                "Foundation commits 50mil USD to fight Ebola | Ethiopian radio and television agency"});
        data.add(new String[]{"The Asian Age",
                "Obama to ask Congress to approve $88 million to boost anti-Ebola effort:"});
    }

    @Override
    public void setUpChild() {
        //siddhiManager.defineStream("define stream NameEntityTypeIn (username string, " +
        // "text string )");

    }

    @Test
    public void testFindNameEntityTypePersonWithoutSuccessiveGroups() throws Exception{
        List<Event> outputEvents = testFindNameEntityType("person", false);

        //expecting words that are of the PERSON type
        String[] expectedMatches = {"John" , "Mahama", "Lizzie", "Bennett", "Obama" , "Barack" , "Obama" , "Bill",
                "Gates", "Obama"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {0,0,1,1,2,2,2,4,4,6};

        assertOutput(outputEvents, expectedMatches, matchedInStreamIndices);
    }

    @Test
    public void testFindNameEntityTypePersonWithSuccessiveGroups() throws Exception{
        List<Event> outputEvents = testFindNameEntityType("PERSON", true);

        //expecting words that are of the PERSON type
        String[] expectedMatches = {"John Mahama" , "Lizzie Bennett", "Obama" , "Barack Obama" , "Bill Gates", "Obama"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {0,1,2,2,4,6};

        assertOutput(outputEvents, expectedMatches, matchedInStreamIndices);
    }

    @Test
    public void testFindNameEntityTypeOrganization() throws Exception{
        List<Event> outputEvents = testFindNameEntityType("ORGANIZATION", false);

        //expecting words that are of the ORGANIZATION type
        String[] expectedMatches = {"Ecowas" , "Congress"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {0,6};

        assertOutput(outputEvents, expectedMatches, matchedInStreamIndices);
    }

    @Test
    public void testFindNameEntityTypeLocation() throws Exception{
        List<Event> outputEvents = testFindNameEntityType("LOCATION", false);

        //expecting words that are of the LOCATION type
        String[] expectedMatches = {"West" , "Africa", "Morocco", "Atlanta", "Malaysia", "Malaysia"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {1,1,1,2,3,3};

        assertOutput(outputEvents, expectedMatches, matchedInStreamIndices);
    }

    @Test
    public void testFindNameEntityTypeDate() throws Exception{
        List<Event> outputEvents = testFindNameEntityType("DATE", false);

        //expecting words that are of the DATE type
        String[] expectedMatches = {"Tuesday" , "Tuesday"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {2,2};

        assertOutput(outputEvents, expectedMatches, matchedInStreamIndices);
    }

    @Test
    public void testFindNameEntityTypeMoney() throws Exception{
        List<Event> outputEvents = testFindNameEntityType("MONEY", true);

        //expecting words that are of the MONEY type
        String[] expectedMatches = {"# 31million" , "$ 88 million"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndex = {4,6};

        for (int i = 0; i < outputEvents.size(); i++){
            Event event = outputEvents.get(i);
            //Compare expected match and returned match
            assertEquals(expectedMatches[i], event.getData(0));
            //Compare expected username and returned username
            assertEquals(data.get(matchedInStreamIndex[i])[0], event.getData(1));
            //Compare expected text and returned text
            assertEquals(data.get(matchedInStreamIndex[i])[1], event.getData(2));
        }
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionInvalidNoOfParams() {
        logger.info("Test: QueryCreationException at Invalid No Of Params");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(defineStream + " from NameEntityTypeIn#nlp:findNameEntityType" +
                "        ( 'PERSON', text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeResult;\n");
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionEntityTypeTypeMismatch(){
        logger.info("Test: QueryCreationException at EntityType type mismatch");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(defineStream +"from NameEntityTypeIn#nlp:findNameEntityType" +
                "        ( 124 , false, text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeResult;\n");
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionGroupSuccessiveEntitiesTypeMismatch(){
        logger.info("Test: QueryCreationException at GroupSuccessiveEntities type mismatch");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(defineStream +"from NameEntityTypeIn#nlp:findNameEntityType" +
                "        ( 'PERSON' , 'false', text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeResult;\n");
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionUndefinedEntityType(){
        logger.info("Test: QueryCreationException at undefined EntityType");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(defineStream +"from NameEntityTypeIn#nlp:findNameEntityType" +
                "        ( 'DEGREE' , false, text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeResult;\n");
    }

    private List<Event> testFindNameEntityType(String entityType, boolean groupSuccessiveEntities) throws Exception{
        logger.info(String.format("Test: EntityType = %s GroupSuccessiveEntities = %b", entityType,
                groupSuccessiveEntities));
        String query = defineStream +
                "@info(name = 'query1') from NameEntityTypeIn#nlp:findNameEntityType" +
                "( '%s' , %b, text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeResult;\n";
        start = System.currentTimeMillis();
        //String queryReference = siddhiManager.addQuery(String.format(query, entityType, groupSuccessiveEntities));
        logger.info("time" + start);
        logger.info(String.format(query, entityType, groupSuccessiveEntities));
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(String.format(query, entityType, groupSuccessiveEntities));
        end = System.currentTimeMillis();

        logger.info(String.format("Time to add query: [%f sec]", ((end - start)/1000f)));

        final List<Event> eventList = new ArrayList<Event>();

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    Collections.addAll(eventList,event);
                }
            }
        });
        executionPlanRuntime.start();
        generateEvents(executionPlanRuntime);

        return eventList;
    }

    private void generateEvents(ExecutionPlanRuntime executionPlanRuntime) throws Exception {
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("NameEntityTypeIn");
        for (Object[] dataLine : data) {
            inputHandler.send(dataLine);
        }
    }

    private void assertOutput(List<Event> outputEvents, String[] expectedMatches, int[] inStreamIndices){
        for (int i = 0; i < outputEvents.size(); i++){
            Event event = outputEvents.get(i);
            //Compare expected output stream match and received match
            assertEquals(expectedMatches[i], event.getData(0));
            //Compare expected output stream username and received username
            assertEquals(data.get(inStreamIndices[i])[0], event.getData(1));
            //Compare expected output stream text and received text
            assertEquals(data.get(inStreamIndices[i])[1], event.getData(2));
        }
    }
}