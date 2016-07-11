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

public class SemgrexPatternStreamProcessorTestCase extends NlpTransformProcessorTestCase {
    private static Logger logger = Logger.getLogger(SemgrexPatternStreamProcessorTestCase.class);
    private static String defineStream = "define stream SemgrexPatternIn(username string, text string);";
    static List<String[]> data = new ArrayList<String[]>();

    @BeforeClass
    public static void loadData() throws Exception {

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
    public void testFindSemgrexPatternMatch() throws Exception {
        //expecting matches
        String[] expectedSubjects = {"Dies", "dies", "died", "died", "died", "died"};
        //expecting namedRelation
        String expectedNamedRelation = "nsubj";
        //expecting namedNode
        String[] expectedNamedNode = {"Doctor", "doctor", "colleagues", "5", "nurses", "workers"};
        //InStream event index for each expected match defined above
        int[] matchedInStreamIndices = {0, 2, 3, 4, 5, 5};

        List<Event> outputEvents = testFindSemgrexPatternMatch("{lemma:die} >/.*subj|num.*/=reln {}=diedsubject");

        for (int i = 0; i < outputEvents.size(); i++) {
            Event event = outputEvents.get(i);
            //Compare expected subject and received subject
            assertEquals(expectedSubjects[i], event.getData(2));
            //Compare expected object and received object
            assertEquals(expectedNamedRelation, event.getData(3));
            //Compare expected verb and received verb
            assertEquals(expectedNamedNode[i], event.getData(4));
            //Compare expected output stream username and received username
            assertEquals(data.get(matchedInStreamIndices[i])[0], event.getData(0));
            //Compare expected output stream text and received text
            assertEquals(data.get(matchedInStreamIndices[i])[1], event.getData(1));
        }
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionInvalidNoOfParams() {
        logger.info("Test: QueryCreationException at Invalid No Of Params");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from SemgrexPatternIn#nlp:findSemgrexPattern" +
                "        ( text) \n" +
                "        select *  \n" +
                "        insert into FindSemgrexPatternResult;\n");
    }

    @Test(expected = ExecutionPlanValidationException.class)
    public void testQueryCreationExceptionRegexCannotParse() {
        logger.info("Test: QueryCreationException at Regex parsing");
        siddhiManager.createExecutionPlanRuntime(defineStream + "from SemgrexPatternIn#nlp:findSemgrexPattern" +
                "        ( '({}=govenor >/.*subj|agent//=reln {}=dependent)',text) \n" +
                "        select *  \n" +
                "        insert into FindSemgrexPatternResult;\n");
    }

    private List<Event> testFindSemgrexPatternMatch(String regex) throws Exception {
        logger.info(String.format("Test: Regex = %s", regex
        ));
        String query = "@info(name = 'query1') from SemgrexPatternIn#nlp:findSemgrexPattern" +
                "        ( '%s', text ) \n" +
                "        select *  \n" +
                "        insert into FindSemgrexPatternResult;\n";
        return runQuery(defineStream + String.format(query, regex), "query1", "SemgrexPatternIn", data);
    }
}