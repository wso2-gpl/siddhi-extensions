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

public class RelationshipByVerbTransformProcessorTestCase extends NlpTransformProcessorTestCase {
    private static Logger logger = Logger.getLogger(RelationshipByVerbTransformProcessorTestCase.class);
    private static List<String[]> data;

    @Override
    public void setUpChild() {
        siddhiManager.defineStream("define stream  RelationshipByVerbIn(username string, text string )");
    }

    @BeforeClass
    public static void loadData() throws Exception {
        data = new ArrayList<String[]>();

        data.add(new String[]{"Democracy Now!",
                "@Laurie_Garrett says the world response to Ebola outbreak is extremely slow & lacking."});
        data.add(new String[]{"Zul",
                "No Ebola cases in the country, says Ministry of Health Malaysia"});
        data.add(new String[]{"Mainstreamedia",
                "Precaution taken though patient does not have all Ebola symptoms, says minister"});
        data.add(new String[]{"Charlie Lima Whiskey",
                "Not Ebola, ministry says of suspected case in Sarawak  via @sharethis"});
        data.add(new String[]{"Bob Ottenhoff",
                "Scientists say Ebola outbreak in West Africa likely to last 12 to 18 months more & could infect " +
                        "hundreds of thousands"});
        data.add(new String[]{"TurnUp Africa",
                "Information just reaching us says another Liberian With Ebola Arrested At Lagos Airport"});
        data.add(new String[]{"_newsafrica",
                "Sierra Leone Says Ebola Saps Revenue, Hampers Growth"});
        data.add(new String[]{"susan schulman",
                "An aid worker says #Ebola outbreak in Liberia demands global help"});
        data.add(new String[]{"Naoko Aoki",
                "Story says virologist was asked to return to Ebola area w/o pay. Hope I'm missing something  via " +
                        "@washingtonpost"});
        data.add(new String[]{"Marc Antoine",
                "U.S. scientists say Ebola epidemic will rage for another 12-18 months"});
        data.add(new String[]{"UMI Wast",
                "Massive global response needed to prevent Ebola infection, say experts"});

    }

    @Test
    public void testRelationshipByVerb() throws Exception{
        //expecting subjects
        String[] expectedSubjects = {"@Laurie_Garrett" , "cases", "Precaution", "ministry" , "Scientists",
                "Information", "Leone", "worker", "Story", "scientists", "response"};
        //expecting objects
        String[] expectedObjects = {null , "Ministry", "minister", null , "outbreak", "Liberian", "Revenue",
                "outbreak", null, null, "experts"};
        //expecting verbs
        String[] expectedVerbs = {"says" , "says", "says", "says" , "say", "says", "Says", "says", "says", "say", "say"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {0,1,2,3,4,5,6,7,8,9,10};

        List<Event> outputEvents = testRelationshipByVerb("say");

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
        siddhiManager.addQuery("from RelationshipByVerbIn#transform.nlp:findRelationshipByVerb" +
                "        ( text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByVerbResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionVerbTypeMismatch(){
        logger.info("Test: QueryCreationException at EntityType type mismatch");
        siddhiManager.addQuery("from RelationshipByVerbIn#transform.nlp:findRelationshipByVerb" +
                "        ( 1,text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByVerbResult;\n");
    }

    private List<Event> testRelationshipByVerb(String regex) throws Exception{
        logger.info(String.format("Test: Verb = %s",regex
        ));
        String query = "from RelationshipByVerbIn#transform.nlp:findRelationshipByVerb" +
                "        ( '%s', text ) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByVerbResult;\n";
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
        InputHandler inputHandler = siddhiManager.getInputHandler("RelationshipByVerbIn");
        for(String[] dataLine:data) {
            inputHandler.send(new Object[]{dataLine[0], dataLine[1]});
        }
    }
}