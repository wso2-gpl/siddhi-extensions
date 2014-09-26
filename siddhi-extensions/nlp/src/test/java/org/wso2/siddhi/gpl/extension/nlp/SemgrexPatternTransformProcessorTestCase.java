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

public class SemgrexPatternTransformProcessorTestCase extends NlpTransformProcessorTestCase {
    private static Logger logger = Logger.getLogger(SemgrexPatternTransformProcessorTestCase.class);
    private static List<String[]> data;

    @Override
    public void setUpChild() {
        siddhiManager.defineStream("define stream  SemgrexPatternIn(username string, text string )");
    }

    @BeforeClass
    public static void loadData() throws Exception {
        data = new ArrayList<String[]>();

        data.add(new String[]{"Leighton Early",
                "4th Doctor Dies of Ebola in Sierra Leone"});
        data.add(new String[]{"☺Brenda Muller",
                "If the Ebola Virus Goes Airborne, 1.2 million Will Die Expert Predicts -  via"});
        data.add(new String[]{"Berkley Bear",
                "Sierra Leone doctor dies of Ebola after failed evacuation"});
        data.add(new String[]{"Gillian Taylor",
                "BritishRedCross: #Ebola: \"The deputy matron has worked 93 days straight while 23 colleagues have " +
                        "died\""});
        data.add(new String[]{"Takashi Katagiri",
                "These scientists made huge discoveries about Ebola--but 5 died before the paper was published."});
        data.add(new String[]{"Jessica",
                "Over 150 nurses and healthcare workers have died doing their job #ebola @nswnma @GlobalNursesU"});

    }

    @Test
    public void testFindSemgrexPatternMatch() throws Exception{
        //expecting matches
        String[] expectedSubjects = {"Dies" , "dies", "died", "died" , "died", "died"};
        //expecting namedRelation
        String expectedNamedRelation = "nsubj";
        //expecting namedNode
        String[] expectedNamedNode = {"Doctor" , "doctor", "colleagues", "5" , "nurses", "workers"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {0,2,3,4,5,5};

        List<Event> outputEvents = testFindSemgrexPatternMatch("{lemma:die} >/.*subj|num.*/=reln {}=diedsubject");

        for (int i = 0; i < outputEvents.size(); i++){
            Event event = outputEvents.get(i);
            //Compare expected subject and received subject
            assertEquals(expectedSubjects[i], event.getData(0));
            //Compare expected object and received object
            assertEquals(expectedNamedRelation, event.getData(1));
            //Compare expected verb and received verb
            assertEquals(expectedNamedNode[i], event.getData(2));
            //Compare expected output stream username and received username
            assertEquals(data.get(matchedInStreamIndices[i])[0], event.getData(3));
            //Compare expected output stream text and received text
            assertEquals(data.get(matchedInStreamIndices[i])[1], event.getData(4));
        }
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionInvalidNoOfParams() {
        logger.info("Test: QueryCreationException at Invalid No Of Params");
        siddhiManager.addQuery("from SemgrexPatternIn#transform.nlp:findSemgrexPattern" +
                "        ( text) \n" +
                "        select *  \n" +
                "        insert into FindSemgrexPatternResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionRegexCannotParse(){
        logger.info("Test: QueryCreationException at Regex parsing");
        siddhiManager.addQuery("from SemgrexPatternIn#transform.nlp:findSemgrexPattern" +
                "        ( '({}=govenor >/.*subj|agent//=reln {}=dependent)',text) \n" +
                "        select *  \n" +
                "        insert into FindSemgrexPatternResult;\n");
    }

    private List<Event> testFindSemgrexPatternMatch(String regex) throws Exception{
        logger.info(String.format("Test: Regex = %s",regex
        ));
        String query = "from SemgrexPatternIn#transform.nlp:findSemgrexPattern" +
                "        ( '%s', text ) \n" +
                "        select *  \n" +
                "        insert into FindSemgrexPatternResult;\n";
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
        InputHandler inputHandler = siddhiManager.getInputHandler("SemgrexPatternIn");
        for(String[] dataLine:data) {
            inputHandler.send(new Object[]{dataLine[0], dataLine[1]});
        }
    }
}