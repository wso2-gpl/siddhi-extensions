/*
 * Copyright (C) 2016 WSO2 Inc. (http://wso2.com)
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

package org.wso2.gpl.siddhi.extension.nlp;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.concurrent.atomic.AtomicInteger;

public class SentimentRateTestCase {
    private static Logger logger = Logger.getLogger(SentimentRateTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);

    @Before
    public void init() {
        count.set(0);
    }

    @Test
    public void testProcess() throws Exception {
        logger.info("Sentiment Extension TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();
        String inStreamDefinition = "@config(async = 'true')define stream inputStream (text string);";
        String query = ("@info(name = 'query1') " + "from inputStream "
                + "select nlp:getSentimentRate(text) as isRate " + "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event inEvent : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(1, inEvent.getData(0));
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(-1, inEvent.getData(0));
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(0, inEvent.getData(0));
                    }
                    if (count.get() == 4) {
                        Assert.assertEquals(-1, inEvent.getData(0));
                    }
                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime
                .getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[] { "Jack is a good person." });
        inputHandler.send(new Object[] { "Jack is a bad person." });
        inputHandler.send(new Object[] { "Jack is a good person. Jack is a bad person" });
        inputHandler.send(new Object[] { "What is wrong with these people" });
        Thread.sleep(100);
        executionPlanRuntime.shutdown();
    }
}
