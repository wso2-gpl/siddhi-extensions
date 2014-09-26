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

public class TokensRegexPatternTransformProcessorTestCase extends NlpTransformProcessorTestCase {
    private static Logger logger = Logger.getLogger(TokensRegexPatternTransformProcessorTestCase.class);
    private static List<String[]> data;

    @Override
    public void setUpChild() {
        siddhiManager.defineStream("define stream TokenRegexPatternIn(regex string, text string )");
    }

    @BeforeClass
    public static void loadData() throws Exception {
        data = new ArrayList<String[]>();

        data.add(new String[]{"Professeur Jamelski",
                "Bill Gates donates $31million to fight Ebola http://t.co/Lw8iJUKlmw http://t.co/wWVGNAvlkC"});
        data.add(new String[]{"going info",
                "Trail Blazers owner Paul Allen donates $9million to fight Ebola"});
        data.add(new String[]{"WinBuzzer",
                "Microsoft co-founder Paul Allen donates $9million to help fight Ebola in Africa"});
        data.add(new String[]{"Lillie Lynch",
                "Canada to donate $2.5M for protective equipment http://t.co/uvRcHSYY0e"});
        data.add(new String[]{"Sark Crushing On Me",
                "Bill Gates donate $50 million to fight ebola in west africa http://t.co/9nd3viiZbe"});
    }

    @Test
    public void testFindTokensRegexPatternMatch() throws Exception{
        //expecting matches
        String[] expectedMatches = {"Bill Gates donates $ 31million" ,
                "Paul Allen donates $ 9million",
                "Microsoft co-founder Paul Allen donates $ 9million",
                "Canada to donate $ 2.5" ,
                "Bill Gates donate $ 50 million"};
        //expecting group_1
        String[] expectedGroup_1 = {"Bill Gates", "Paul Allen", "Microsoft", "Canada", "Bill Gates"};
        //expecting group_2
        String[] expectedGroup_2 = {"$ 31million" , "$ 9million", "$ 9million", "$ 2.5" , "$ 50 million"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {0,1,2,3,4};

        List<Event> outputEvents = testFindTokensRegexPatternMatch("([ner:/PERSON|ORGANIZATION|LOCATION/]+) (?:[]* [lemma:donate]) ([ner:MONEY]+)");

        for (int i = 0; i < outputEvents.size(); i++){
            Event event = outputEvents.get(i);
            //Compare expected subject and received subject
            assertEquals(expectedMatches[i], event.getData(0));
            //Compare expected object and received object
            assertEquals(expectedGroup_1[i], event.getData(1));
            //Compare expected verb and received verb
            assertEquals(expectedGroup_2[i], event.getData(2));
            //Compare expected output stream username and received username
            assertEquals(data.get(matchedInStreamIndices[i])[0], event.getData(3));
            //Compare expected output stream text and received text
            assertEquals(data.get(matchedInStreamIndices[i])[1], event.getData(4));
        }
    }

    @Test(expected = org.wso2.siddhi.core.exception.QueryCreationException.class)
    public void testQueryCreationExceptionInvalidNoOfParams() {
        logger.info("Test: QueryCreationException at Invalid No Of Params");
        siddhiManager.addQuery("from TokenRegexPatternIn#transform.nlp:findTokensRegexPattern" +
                "        ( text) \n" +
                "        select *  \n" +
                "        insert into TokenRegexPatternResult;\n");
    }

    @Test(expected = QueryCreationException.class)
    public void testQueryCreationExceptionRegexCannotParse(){
        logger.info("Test: QueryCreationException at Regex parsing");
        siddhiManager.addQuery("from TokenRegexPatternIn#transform.nlp:findTokensRegexPattern" +
                "        ( '/{tag:NP.*}/',text) \n" +
                "        select *  \n" +
                "        insert into TokenRegexPatternResult;\n");
    }

    private List<Event> testFindTokensRegexPatternMatch(String regex) throws Exception{
        logger.info(String.format("Test: Regex = %s",regex
        ));
        String query = "from TokenRegexPatternIn#transform.nlp:findTokensRegexPattern" +
                "        ( '%s', text ) \n" +
                "        select *  \n" +
                "        insert into TokenRegexPatternResult;\n";
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
        InputHandler inputHandler = siddhiManager.getInputHandler("TokenRegexPatternIn");
        for(String[] dataLine:data) {
            inputHandler.send(new Object[]{dataLine[0], dataLine[1]});
        }
    }
}