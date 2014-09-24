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

public class NameEntityTypeViaDictionaryTransformProcessorTestCase extends NlpTransformProcessorTestCase {
    private static Logger logger = Logger.getLogger(NameEntityTypeViaDictionaryTransformProcessorTestCase.class);
    private static List<String[]> data;

    @BeforeClass
    public static void loadData() {
        data = new ArrayList<String[]>();
        data.add(new String[]{"encomium",
                "Patrick Sawyer’s chain of Ebola victims"});
        data.add(new String[]{"Onasis Elom Gaisie",
                "RT @BBCAfrica: #Ghana President John Mahama, the chairman of regional body Ecowas, " +
                        "is on a one day tour of three West African countries wra…"});
        data.add(new String[]{"Surviving Jim",
                "RT @SurvivalBasic: Woman ARRIVING in West Africa From Morocco Tests Positive For Ebola: By Lizzie " +
                        "Bennett A South... …"});
        data.add(new String[]{"Atlanta News",
                "Obama to visit Atlanta Tuesday for Ebola update: President Barack Obama is scheduled Tuesday to...  " +
                        "#Atlanta #GA"});
        data.add(new String[]{"Duchess of Deception",
                "#LRTs hmmmm can't be Ebola ku Dedza but those symptoms do sound suspect. Ebola 2.0 maybe?"});
        data.add(new String[]{"Michael Chavez",
                "Malaysia to send 20 million medical gloves to fight Ebola: Malaysia will send more than 20 million medical rub..."});
        data.add(new String[]{"Professeur Jamelski",
                "RT @DailyMirror: Bill Gates donates £31million to fight Ebola"});
        data.add(new String[]{"ethioembabidjan",
                "Foundation commits 50mil USD to fight Ebola | Ethiopian radio and television agency"});
        data.add(new String[]{"The Asian Age",
                "Obama to ask Congress to approve $88 million to boost anti-Ebola effort:"});
        data.add(new String[]{"Michael Chavez",
                "Obama to detail plans on Ebola offensive on Tuesday: WSJ: U.S. President Barack Obama is expected to detail " +
                        "on..."});
        data.add(new String[]{"Geraldine McCrossan",
                "RT @BAndrewsGOAL: First ebola patient at GOAL supported isolation unit arrived yesterday. Can't have" +
                        " been more than 8 years old …"});
        data.add(new String[]{"Henry L Niman PhD",
                "Liberia MoH releases September 11 #Ebola tally by province #EbolaOutbreak  REDEMPTION HOLDING " +
                        "UPDATE"});
        data.add(new String[]{"Yapping",
                "RT @UrbanCraziness: This soap is $195, it better wash Ebola, wash HIV, wash Malaria, " +
                        "shit.... It better wash all my sins away …"});
        data.add(new String[]{"A K. Owusu-Nyantakyi",
                "RT @AfricaBizz: @CDCgov Foundation gets $9 million grant from Paul G. Allen Family Foundation for  " +
                        "#Ebola response"});
        data.add(new String[]{"#BRINGBACKOURGIRLS#",
                "RT @metronaija: #Nigeria #News Ebola Survivor Dr. Kent Brantly Donates Blood to Treat Another " +
                        "Infected Doctor"});
        data.add(new String[]{"susan schulman",
                "RT @TimesLIVE: African Union sends 30 disease specialists to fight Ebola outbreak in Liberia"});
        data.add(new String[]{"Aaron Beals",
                "RT @WBUR: Boston's Partners In Health is leaping into the Ebola crisis:"});
        data.add(new String[]{"Ecko  Stanislas",
                "RT @JessicaChasmar: Bill Maher compares #GOP Rep. John Kline to Islamic State, Ebola  #tcot"});
        data.add(new String[]{"HC YEOH",
                "EBOLA IN M'SIA: Gov't confirms suspected case , " +
                        "Zimbabwean student quarantined  It was just a matter of time to appear"});
        data.add(new String[]{"going info",
                "Trail Blazers owner Paul Allen donates $9 million to fight Ebola"});
    }


    @Override
    public void setUpChild() {
        siddhiManager.defineStream("define stream NameEntityTypeViaDictionaryIn (username string, text string )");
    }

    @Test
    public void testFindNameEntityTypePerson() throws Exception{
        List<Event> outputEvents = testFindNameEntityTypeViaDictionary("PERSON", "siddhi-extensions/nlp/src/test/resources/dictionary.xml");

        //expecting words that are of the PERSON type
        String[] expectedMatches = {"Obama" , "Bill Gates", "Obama", "Obama", "Kent Brantly", "Paul Allen"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {3,6,8,9,14,19};

        assertOutput(outputEvents, expectedMatches, matchedInStreamIndices);
    }

    @Test
    public void testFindNameEntityTypeLocation() throws Exception{
        List<Event> outputEvents = testFindNameEntityTypeViaDictionary("LOCATION",
                "siddhi-extensions/nlp/src/test/resources/dictionary.xml");

        //expecting words that are of the LOCATION type
        String[] expectedMatches = {"Africa" , "Morocco", "Africa", "Atlanta", "Africa", "Africa"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {1,2,2,3,13,15};

        assertOutput(outputEvents, expectedMatches, matchedInStreamIndices);
    }

    @Test
    public void testFindNameEntityTypeDate() throws Exception{
        List<Event> outputEvents = testFindNameEntityTypeViaDictionary("DATE",
                "siddhi-extensions/nlp/src/test/resources/dictionary.xml");

        //expecting words that are of the DATE type
        String[] expectedMatches = {"Tuesday" , "Tuesday", "yesterday", "September"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {3,9,10,11};

        assertOutput(outputEvents, expectedMatches, matchedInStreamIndices);
    }

    @Test
    public void testFindNameEntityTypeMoney() throws Exception{
        List<Event> outputEvents = testFindNameEntityTypeViaDictionary("MONEY",
                "siddhi-extensions/nlp/src/test/resources/dictionary.xml");

        //expecting words that are of the MONEY type
        String[] expectedMatches = {"million" , "million", "USD", "million", "million", "million", "million"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {5,6,7,8,13,19};

        assertOutput(outputEvents, expectedMatches, matchedInStreamIndices);
    }

    @Test(expected = org.wso2.siddhi.core.exception.QueryCreationException.class)
    public void testQueryCreationExceptionInvalidNoOfParams() {
        logger.info("Test: QueryCreationException at Invalid No Of Params");
        siddhiManager.addQuery("from NameEntityTypeViaDictionaryIn#transform.nlp:findNameEntityTypeViaDictionary" +
                "        ('src/test/resources/dictionaryTest.xml',text) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeViaDictionaryResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionTypeMismatchEntityType(){
        logger.info("Test: QueryCreationException at EntityType type mismatch");
        siddhiManager.addQuery("from NameEntityTypeViaDictionaryIn#transform.nlp:findNameEntityTypeViaDictionary" +
                "        ( 1234,'src/test/resources/dictionaryTest.xml',text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeViaDictionaryResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionInvalidFilePath(){
        logger.info("Test: QueryCreationException at Invalid file path");
        siddhiManager.addQuery("from NameEntityTypeViaDictionaryIn#transform.nlp:findNameEntityTypeViaDictionary" +
                "        ( 'PERSON' , 'src/resources/dictionaryTest.xml', text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeViaDictionaryResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionUndefinedEntityType(){
        logger.info("Test: QueryCreationException at undefined EntityType");
        siddhiManager.addQuery("from NameEntityTypeViaDictionaryIn#transform.nlp:findNameEntityTypeViaDictionary" +
                "        ( 'DEGREE' , 'src/test/resources/dictionaryTest.xml', text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeViaDictionaryResult;\n");
    }


    private List<Event> testFindNameEntityTypeViaDictionary(String entityType, String filePath) throws Exception{
        logger.info(String.format("Test: EntityType = %s", entityType
        ));
        String query = "from NameEntityTypeViaDictionaryIn#transform.nlp:findNameEntityTypeViaDictionary" +
                "        ( '%s' , '%s', text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeViaDictionaryResult;\n";
        start = System.currentTimeMillis();
        String queryReference = siddhiManager.addQuery(String.format(query, entityType, filePath));
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
        InputHandler inputHandler = siddhiManager.getInputHandler("NameEntityTypeViaDictionaryIn");
        for(String[] dataLine:data) {
            inputHandler.send(new Object[]{dataLine[0], dataLine[1]});
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