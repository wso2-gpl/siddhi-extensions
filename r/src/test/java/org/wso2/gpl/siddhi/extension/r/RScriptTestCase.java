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

package org.wso2.gpl.siddhi.extension.r;

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

import static org.junit.Assume.assumeTrue;

public class RScriptTestCase {

    static final Logger log = Logger.getLogger(RScriptTestCase.class);

    protected static SiddhiManager siddhiManager;
    private int count;
    protected double doubleValue;
    protected long longValue;
    protected int intValue;
    protected boolean boolValue;

    @Before
    public void init() {
        count = 0;
        siddhiManager = new SiddhiManager();
    }

    @Test
    public void testRScript1() throws InterruptedException {
        log.info("r:eval test1");
        assumeTrue(System.getenv("JRI_HOME")!=null);

        String defineStream = "@config(async = 'true') define stream weather (time long, temp double); ";

        String executionPlan = defineStream + " @info(name = 'query1') from weather#window.lengthBatch(2)" +
                "#r:eval(\"c <- sum(time); m <- sum(temp); \", \"c long, m double\"," +
                " time, temp)" +
                " select *" +
                " insert into dataOut;";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {

                    for (Event event : inEvents) {
                        longValue = (Long) event.getData(2);
                        doubleValue = (Double) event.getData(3);
                    }
                    count++;
                }
            }
        });
        executionPlanRuntime.start();
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("weather");
        inputHandler.send(new Object[]{10l, 55.6d});
        inputHandler.send(new Object[]{20l, 65.6d});
        inputHandler.send(new Object[]{30l, 75.6d});
        Thread.sleep(1000);
        Assert.assertEquals("Only one event must arrive", 1, count);
        Assert.assertEquals("Value 1 returned", 10 + 20, longValue);
        Assert.assertEquals("Value 2 returned", (55.6 + 65.6), doubleValue, 1e-4);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testRScript2() throws InterruptedException {
        log.info("r:eval test2");
        assumeTrue(System.getenv("JRI_HOME") != null);
        String defineStream = "@config(async = 'true') define stream weather (time int, temp double); ";

        String executionPlan = defineStream + " @info(name = 'query1') from weather#window.timeBatch(2 sec)" +
                "#r:eval('c <- sum(time); m <- sum(temp); ', 'c int, m double', time, temp)" +
                " select *" +
                " insert into dataOut;";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {

                    for (Event event : inEvents) {
                        intValue = (Integer) event.getData(2);
                        doubleValue = (Double) event.getData(3);
                    }
                    count++;
                }
            }
        });

        executionPlanRuntime.start();
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("weather");
        inputHandler.send(new Object[]{10, 55.6});
        inputHandler.send(new Object[]{20, 65.6});
        Thread.sleep(2500);
        inputHandler.send(new Object[]{30, 75.6});
        Thread.sleep(1000);
        Assert.assertEquals("Only one event must arrive", 1, count);
        Assert.assertEquals("Value 1 returned", 30, intValue);
        Assert.assertEquals("Value 2 returned", (55.6 + 65.6), doubleValue, 1e-4);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testRScript3() throws InterruptedException {
        log.info("r:eval test3");
        assumeTrue(System.getenv("JRI_HOME") != null);

        String defineStream = "@config(async = 'true') define stream weather (time int, temp bool); ";

        String executionPlan = defineStream + " @info(name = 'query1') from weather#window.lengthBatch(3)" +
                "#r:eval(\"c <- sum(time); m <- any(temp); \", \"c double, m bool\"," +
                " time, temp)" +
                " select *" +
                " insert into dataOut;";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {

                    for (Event event : inEvents) {
                        doubleValue = (Double) event.getData(2);
                        boolValue = (Boolean) event.getData(3);
                    }
                    count++;
                }
            }
        });

        executionPlanRuntime.start();
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("weather");
        inputHandler.send(new Object[]{10, true});
        inputHandler.send(new Object[]{20, false});
        inputHandler.send(new Object[]{30, true});
        Thread.sleep(1000);
        inputHandler.send(new Object[]{40, false});

        Assert.assertEquals("Only one event must arrive", 1, count);
        Assert.assertEquals("Value 1 returned", (10 + 20 +30) + 0.0, doubleValue, 1e-4);
        Assert.assertEquals("Value 2 returned", true, boolValue);
        executionPlanRuntime.shutdown();
    }

}
