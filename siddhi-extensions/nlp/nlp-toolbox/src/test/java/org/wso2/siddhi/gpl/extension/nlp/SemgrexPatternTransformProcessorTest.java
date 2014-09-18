/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.siddhi.gpl.extension.nlp;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class SemgrexPatternTransformProcessorTest extends NlpTransformProcessorTest {
    private static Logger logger = Logger.getLogger(SemgrexPatternTransformProcessorTest.class);


    @Override
    public void setUpChild() {
        siddhiManager.defineStream("define stream  SemgrexPatternIn(regex string, text string )");
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

    @Test
    public void testFindSemgrexPatternMatch() throws Exception{

        testFindSemgrexPatternMatch("({}=govenor >/.*subj|agent/=reln {}=dependent)");
    }


    private void testFindSemgrexPatternMatch(String regex) throws Exception{
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

        siddhiManager.addCallback(queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                System.out.println
                        ("========================================================================================================================================================================================");
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event:inEvents){
                    Event[] subEventArray = event.toArray();
                    if (subEventArray != null){
                        for (Event subEvent:subEventArray){
                            System.out.println
                                    ("---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
                            System.out.println("timestamp="+ subEvent.getTimeStamp());
                            System.out.print("data=[");
                            for (Object obj: subEvent.getData()){
                                System.out.print(obj + ",");
                            }
                            System.out.println("]");
                        }
                    }
                }
            }
        });

        generateEvents();
    }

    private void generateEvents() throws Exception{
        InputHandler inputHandler = siddhiManager.getInputHandler("SemgrexPatternIn");
        for(String[] dataLine:data) {
            inputHandler.send(new Object[]{dataLine[0], dataLine[1]});
        }
    }
}