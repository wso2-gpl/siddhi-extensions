/*
 * Copyright (c) 2005 - 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.wso2.siddhi.gpl.extension.evalscript;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.function.exceptions.FunctionInitializationException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class EvalRTestCase {

    static final Logger log = Logger.getLogger(EvalRTestCase.class);

    boolean isReceived[] = new boolean[10];
    Object value[] = new Object[10];

    @Test
    public void testEvalRConcat() throws InterruptedException {

        log.info("TestEvalRConcat");

        SiddhiManager siddhiManager = new SiddhiManager();

        String concatFunc = "define function concatR[R] return string {\n" +
                "return(paste(data, collapse=\"\"));" +
                "};";

        String cseEventStream = "@config(async = 'true')define stream cseEventStream (symbol string, price float, volume long);";
        String query = ("@info(name = 'query1') from cseEventStream select price , concatR(symbol,' ',price) as concatStr " +
                "group by volume insert into mailOutput;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(concatFunc + cseEventStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                isReceived[0] = true;
                value[0] = inEvents[inEvents.length - 1].getData(1);
            }
        });

        isReceived[0] = false;
        value[0] = null;

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 100l});
        Thread.sleep(100);

        if (isReceived[0]) {
            Assert.assertEquals("IBM 700", value[0]);
        } else {
            throw new RuntimeException("The event has not been received");
        }

        executionPlanRuntime.shutdown();
    }

    @Test(expected = FunctionInitializationException.class)
    public void testRCompilationFailure() throws InterruptedException {

        log.info("testRCompilationFailure");

        SiddhiManager siddhiManager = new SiddhiManager();

        String concatFunc = "define function concatR[R] return string {\n" +
                "  str1 <- data[1;\n" +
                "  str2 <- data[2];\n" +
                "  str3 <- data[3];\n" +
                "  res <- str1;\n" +
                "  return res;\n" +
                "};";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(concatFunc);

        executionPlanRuntime.shutdown();
    }
}
