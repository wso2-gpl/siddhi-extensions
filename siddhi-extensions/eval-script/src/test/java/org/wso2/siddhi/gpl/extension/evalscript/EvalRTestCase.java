
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

package org.wso2.siddhi.gpl.extension.evalscript;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.extension.evalscript.exceptions.FunctionInitializationException;

import static org.junit.Assume.assumeTrue;

public class EvalRTestCase {

    static final Logger log = Logger.getLogger(EvalRTestCase.class);

    boolean isReceived[] = new boolean[10];
    Object value[] = new Object[10];

    @Test
    public void testEvalRConcat() throws InterruptedException {
        log.info("TestEvalRConcat");
        assumeTrue(System.getenv("JRI_HOME")!=null);

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
        assumeTrue(System.getenv("JRI_HOME")!=null);

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
