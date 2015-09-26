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

import static org.junit.Assert.assertEquals;

public class RelationshipByRegexStreamProcessorTestCase extends NlpTransformProcessorTestCase {
    private static Logger logger = Logger.getLogger(RelationshipByRegexStreamProcessorTestCase.class);
    private static String defineStream = "define stream RelationshipByRegexIn(username string, text string);";
    static List<String[]> data = new ArrayList<String[]>();

    @BeforeClass
    public static void loadData() throws Exception {

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

    @Test
    public void testRelationshipByRegex() throws Exception {
        //expecting subjects
        String[] expectedSubjects = {"Gates", "Allen", "Allen", "Malaysia", "@gatesfoundation", "Ellen"};
        //expecting objects
        String[] expectedObjects = {"31million", "9million", "9million", "20.9", "50M", "Bags"};
        //expecting verbs
        String[] expectedVerbs = {"donates", "donates", "donates", "donate", "donates", "Donates"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {0, 1, 2, 3, 4, 5};

        List<Event> outputEvents = testRelationshipByRegex("({} </xcomp|dobj/ ({lemma:donate}=verb >nsubj {}=subject)" +
                ") >/nsubj|num/ {}=object");

        for (int i = 0; i < outputEvents.size(); i++) {
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

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionInvalidNoOfParams() {
        logger.info("Test: QueryCreationException at Invalid No Of Params");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from RelationshipByRegexIn#nlp:findRelationshipByRegex" +
                "        (text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n");
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionRegexNotContainVerb() {
        logger.info("Test: QueryCreationException at Regex does not contain Verb");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from RelationshipByRegexIn#nlp:findRelationshipByRegex" +
                "        ( '{} >/nsubj|agent/ {}=subject ?>/dobj/ {}=object',text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n");
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionRegexNotContainSubject() {
        logger.info("Test: QueryCreationException at Regex does not contain Subject");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from RelationshipByRegexIn#nlp:findRelationshipByRegex" +
                "        ('{}=verb >/nsubj|agent/ {} ?>/dobj/ {}=object',text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n");
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionRegexNotContainObject() {
        logger.info("Test: QueryCreationException at Regex does not contain Object");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from RelationshipByRegexIn#nlp:findRelationshipByRegex" +
                "        ('{}=verb >/nsubj|agent/ {}=subject ?>/dobj/ {}',text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n");
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionCannotParseRegex() {
        logger.info("Test: QueryCreationException at Regex does not contain Object");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from RelationshipByRegexIn#nlp:findRelationshipByRegex" +
                "        ('{}=verb ??>/nsubj|agent/ {}=subject ?>/dobj/ {}',text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n");
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionRegexTypeMismatch() {
        logger.info("Test: QueryCreationException at Regex parsing");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from RelationshipByRegexIn#nlp:findRelationshipByRegex" +
                "        ('{}=verb >/nsubj|agent/ {}=subject ?>/dobj/ {}',text) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n");
    }


    private List<Event> testRelationshipByRegex(String regex) throws Exception {
        logger.info(String.format("Test: Regex = %s", regex
        ));
        String query = "@info(name = 'query1') from RelationshipByRegexIn#nlp:findRelationshipByRegex" +
                "        ( '%s', text ) \n" +
                "        select *  \n" +
                "        insert into FindRelationshipByRegexResult;\n";
        return runQuery(defineStream + String.format(query, regex), "query1", "RelationshipByRegexIn", data);
    }
}