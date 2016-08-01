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

public class RSourceTestCase {

    static final Logger log = Logger.getLogger(RSourceTestCase.class);

    protected static SiddhiManager siddhiManager = new SiddhiManager();
    private int count;
    protected double value1;
    protected double value2;
    protected boolean valueBool;
    protected String valueString;
    protected float valueFloat;
    protected long valueLong;

    @Before
    public void init() {
        count = 0;
    }

    // get double values to the output stream
    @Test
    public void testRSource1() throws InterruptedException {
        log.info("r:evalSource test1");
        assumeTrue(System.getenv("JRI_HOME")!=null);

        String defineStream = "@config(async = 'true') define stream weather (time long, temp double); ";

        String executionPlan = defineStream + " @info(name = 'query1') from weather#window.timeBatch(1 sec)" +
                "#r:evalSource(\"src/test/resources/sample.R\", \"m double, c long\"," +
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
                        value1 = (Double) event.getData(2);
                        valueLong = (Long) event.getData(3);
                    }
                    count++;
                }
            }
        });

        executionPlanRuntime.start();
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("weather");
        inputHandler.send(new Object[]{10l, 55.6d});
        inputHandler.send(new Object[]{20l, 65.6d});
        Thread.sleep(2000);
        inputHandler.send(new Object[]{30l, 75.6d});
        Thread.sleep(500);
        Assert.assertEquals("Only one event must arrive", 1, count);
        Assert.assertEquals("Value 1 returned", 121.2, value1, 1e-4);
        Assert.assertEquals("Value 2 returned", 30l, valueLong, 1e-4);
        executionPlanRuntime.shutdown();
    }

    // get integer, float values to the output stream
    @Test
    public void testRSource2() throws InterruptedException {
        log.info("r:evalSource test2");
        assumeTrue(System.getenv("JRI_HOME") != null);

        String defineStream = "@config(async = 'true') define stream weather (time long, temp double); ";

        String executionPlan = defineStream + " @info(name = 'query1') from weather#window.lengthBatch(2)" +
                "#r:evalSource(\"src/test/resources/sample2.R\", \"m int, c float\"," +
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
                        value1 = (Integer) event.getData(2);
                        valueFloat = (Float) event.getData(3);
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
        Assert.assertEquals("Value 1 returned", 121, value1, 1e-4);
        Assert.assertEquals("Value 2 returned", 30f, valueFloat);
        executionPlanRuntime.shutdown();
    }

    // get string, bool to the output stream
    @Test
    public void testRSource3() throws InterruptedException {
        log.info("r:evalSource test3");
        assumeTrue(System.getenv("JRI_HOME") != null);

        String defineStream = "@config(async = 'true') define stream weather (time long, temp double); ";

        String executionPlan = defineStream + " @info(name = 'query1') from weather#window.lengthBatch(2)" +
                "#r:evalSource(\"src/test/resources/sample3.R\", \"c string, m bool\"," +
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
                        valueString = (String) event.getData(2);
                        valueBool = (Boolean) event.getData(3);
                    }
                    count++;
                }
            }
        });

        executionPlanRuntime.start();
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("weather");
        inputHandler.send(new Object[]{123l, 55.6d});
        inputHandler.send(new Object[]{101l, 72.3d});
        Thread.sleep(1000);
        Assert.assertEquals("Only one event must arrive", 1, count);
        Assert.assertEquals("Value 1 returned", "178.6", valueString);
        Assert.assertEquals("Value 2 returned", true, valueBool);
        executionPlanRuntime.shutdown();
    }

}
