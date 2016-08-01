/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.gpl.siddhi.extension.nlp;

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

        List<Event> outputEvents = testRelationshipByRegex(
                "({} </compound|dobj/ ({lemma:donate}=verb >nsubj {}=subject)) >/nsubj|num/ {}=object");

        for (int i = 0; i < outputEvents.size(); i++) {
            Event event = outputEvents.get(i);
            //Compare expected subject and received subject
            assertEquals(expectedSubjects[i], event.getData(2));
            //Compare expected object and received object
            assertEquals(expectedObjects[i], event.getData(3));
            //Compare expected verb and received verb
            assertEquals(expectedVerbs[i], event.getData(4));
            //Compare expected output stream username and received username
            assertEquals(data.get(matchedInStreamIndices[i])[0], event.getData(0));
            //Compare expected output stream text and received text
            assertEquals(data.get(matchedInStreamIndices[i])[1], event.getData(1));
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