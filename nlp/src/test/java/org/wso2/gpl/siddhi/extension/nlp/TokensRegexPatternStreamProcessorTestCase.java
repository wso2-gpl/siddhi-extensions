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

public class TokensRegexPatternStreamProcessorTestCase extends NlpTransformProcessorTestCase {
    private static Logger logger = Logger.getLogger(TokensRegexPatternStreamProcessorTestCase.class);
    private static String defineStream = "define stream TokenRegexPatternIn(regex string, text string);";
    static List<String[]> data = new ArrayList<String[]>();

    @BeforeClass
    public static void loadData() throws Exception {

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
    public void testFindTokensRegexPatternMatch() throws Exception {
        //expecting matches
        String[] expectedMatches = {"Bill Gates donates $31million",
                "Paul Allen donates $9million",
                "Microsoft co-founder Paul Allen donates $9million",
                "Canada to donate $2.5",
                "Bill Gates donate $50 million"};
        //expecting group_1
        String[] expectedGroup_1 = {"Bill Gates", "Paul Allen", "Microsoft", "Canada", "Bill Gates"};
        //expecting group_2
        String[] expectedGroup_2 = {"$31million", "$9million", "$9million", "$2.5", "$50 million"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {0, 1, 2, 3, 4};

        List<Event> outputEvents = testFindTokensRegexPatternMatch("([ner:/PERSON|ORGANIZATION|LOCATION/]+) (?:[]* [lemma:donate]) ([ner:MONEY]+)");

        for (int i = 0; i < outputEvents.size(); i++) {
            Event event = outputEvents.get(i);
            //Compare expected subject and received subject
            assertEquals(expectedMatches[i], event.getData(2));
            //Compare expected object and received object
            assertEquals(expectedGroup_1[i], event.getData(3));
            //Compare expected verb and received verb
            assertEquals(expectedGroup_2[i], event.getData(4));
            //Compare expected output stream username and received username
            assertEquals(data.get(matchedInStreamIndices[i])[0], event.getData(0));
            //Compare expected output stream text and received text
            assertEquals(data.get(matchedInStreamIndices[i])[1], event.getData(1));
        }
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionInvalidNoOfParams() {
        logger.info("Test: QueryCreationException at Invalid No Of Params");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from TokenRegexPatternIn#nlp:findTokensRegexPattern" +
                "        ( text) \n" +
                "        select *  \n" +
                "        insert into TokenRegexPatternResult;\n");
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionRegexCannotParse() {
        logger.info("Test: QueryCreationException at Regex parsing");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from TokenRegexPatternIn#nlp:findTokensRegexPattern" +
                "        ( '/{tag:NP.*}/',text) \n" +
                "        select *  \n" +
                "        insert into TokenRegexPatternResult;\n");
    }

    private List<Event> testFindTokensRegexPatternMatch(String regex) throws Exception {
        logger.info(String.format("Test: Regex = %s", regex));
        String query = "@info(name = 'query1') from TokenRegexPatternIn#nlp:findTokensRegexPattern" +
                "        ( '%s', text ) \n" +
                "        select *  \n" +
                "        insert into TokenRegexPatternResult;\n";
        start = System.currentTimeMillis();
        return runQuery(defineStream + String.format(query, regex), "query1", "TokenRegexPatternIn", data);
    }
}