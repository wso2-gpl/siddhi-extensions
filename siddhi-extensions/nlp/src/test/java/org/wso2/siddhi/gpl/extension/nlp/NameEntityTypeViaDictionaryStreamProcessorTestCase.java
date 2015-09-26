/*
 * Copyright (C) 2015 WSO2 Inc. (http://wso2.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.wso2.siddhi.gpl.extension.nlp;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.List;

public class NameEntityTypeViaDictionaryStreamProcessorTestCase extends NlpTransformProcessorTestCase {
    private static Logger logger = Logger.getLogger(NameEntityTypeViaDictionaryStreamProcessorTestCase.class);
    private static String defineStream = "define stream NameEntityTypeViaDictionaryIn (username string, text string);";
    static List<String[]> data = new ArrayList<String[]>();

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


    @Test
    public void testFindNameEntityTypePerson() throws Exception {
        String dictionaryFilePath = this.getClass().getClassLoader().getResource("dictionary.xml").getPath();
        List<Event> outputEvents = testFindNameEntityTypeViaDictionary("PERSON", dictionaryFilePath);

        //expecting words that are of the PERSON type
        String[] expectedMatches = {"Obama", "Bill Gates", "Obama", "Obama", "Kent Brantly", "Paul Allen"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {3, 6, 8, 9, 14, 19};

        assertOutput(outputEvents, expectedMatches, matchedInStreamIndices, data);
    }

    @Test
    public void testFindNameEntityTypeLocation() throws Exception {
        String dictionaryFilePath = this.getClass().getClassLoader().getResource("dictionary.xml").getPath();
        List<Event> outputEvents = testFindNameEntityTypeViaDictionary("LOCATION", dictionaryFilePath);

        //expecting words that are of the LOCATION type
        String[] expectedMatches = {"Africa", "Morocco", "Africa", "Atlanta", "Africa", "Africa"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {1, 2, 2, 3, 13, 15};

        assertOutput(outputEvents, expectedMatches, matchedInStreamIndices, data);
    }

    @Test
    public void testFindNameEntityTypeDate() throws Exception {
        String dictionaryFilePath = this.getClass().getClassLoader().getResource("dictionary.xml").getPath();
        List<Event> outputEvents = testFindNameEntityTypeViaDictionary("DATE", dictionaryFilePath);

        //expecting words that are of the DATE type
        String[] expectedMatches = {"Tuesday", "Tuesday", "yesterday", "September"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {3, 9, 10, 11};

        assertOutput(outputEvents, expectedMatches, matchedInStreamIndices, data);
    }

    @Test
    public void testFindNameEntityTypeMoney() throws Exception {
        String dictionaryFilePath = this.getClass().getClassLoader().getResource("dictionary.xml").getPath();
        List<Event> outputEvents = testFindNameEntityTypeViaDictionary("MONEY", dictionaryFilePath);

        //expecting words that are of the MONEY type
        String[] expectedMatches = {"million", "million", "USD", "million", "million", "million", "million"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {5, 6, 7, 8, 13, 19};

        assertOutput(outputEvents, expectedMatches, matchedInStreamIndices, data);
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionInvalidNoOfParams() {
        logger.info("Test: QueryCreationException at Invalid No Of Params");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from NameEntityTypeViaDictionaryIn#nlp:findNameEntityTypeViaDictionary" +
                "        ('src/test/resources/dictionaryTest.xml',text) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeViaDictionaryResult;\n");
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionTypeMismatchEntityType() {
        logger.info("Test: QueryCreationException at EntityType type mismatch");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from NameEntityTypeViaDictionaryIn#nlp:findNameEntityTypeViaDictionary" +
                "        ( 1234,'src/test/resources/dictionaryTest.xml',text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeViaDictionaryResult;\n");
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionInvalidFilePath() {
        logger.info("Test: QueryCreationException at Invalid file path");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from NameEntityTypeViaDictionaryIn#nlp:findNameEntityTypeViaDictionary" +
                "        ( 'PERSON' , 'src/resources/dictionaryTest.xml', text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeViaDictionaryResult;\n");
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionUndefinedEntityType() {
        logger.info("Test: QueryCreationException at undefined EntityType");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from NameEntityTypeViaDictionaryIn#nlp:findNameEntityTypeViaDictionary" +
                "        ( 'DEGREE' , 'src/test/resources/dictionaryTest.xml', text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeViaDictionaryResult;\n");
    }


    private List<Event> testFindNameEntityTypeViaDictionary(String entityType, String filePath) throws Exception {
        logger.info(String.format("Test: EntityType = %s", entityType));
        String query = "@info(name = 'query1') from NameEntityTypeViaDictionaryIn#nlp:findNameEntityTypeViaDictionary" +
                "        ( '%s' , '%s', text ) \n" +
                "        select *  \n" +
                "        insert into FindNameEntityTypeViaDictionaryResult;\n";

        return runQuery(defineStream + String.format(query, entityType, filePath), "query1", "NameEntityTypeViaDictionaryIn", data);
    }
}