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
package org.wso2.gpl.siddhi.extensions.geo.stream;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.gpl.siddhi.extensions.geo.GeoTestCase;

import java.util.ArrayList;

public class GeoProximityTestCase extends GeoTestCase {
    private static Logger logger = Logger.getLogger(GeoProximityTestCase.class);
    ArrayList<String> expectedResultId;
    @Test
     public void testProximity() throws Exception {
        logger.info("TestProximity");

        data.clear();
        expectedResultId = new ArrayList<String>();
        eventCount = 0;

        data.add(new Object[]{"1", 0d, 0d});
        data.add(new Object[]{"2", 1d, 1d});
        data.add(new Object[]{"3", 2d, 2d});
        data.add(new Object[]{"1", 1.5d, 1.5d});
        expectedResultId.add("2");
        expectedResult.add(true);
        expectedResultId.add("3");
        expectedResult.add(true);
        data.add(new Object[]{"1", 1.6d, 1.6d});
        data.add(new Object[]{"2", 5d, 5d});
        expectedResultId.add("1");
        expectedResult.add(false);
        data.add(new Object[]{"1", 2d, 2d});
        data.add(new Object[]{"1", 5.5d, 5.5d});
        expectedResultId.add("2");
        expectedResult.add(true);
        expectedResultId.add("3");
        expectedResult.add(false);

        String executionPlan = "@config(async = 'true')" +
                "define stream dataIn (id string, longitude double, latitude double);"
                +
                "@info(name = 'query1') " +
                "from dataIn#geo:proximity(id,longitude,latitude, 110574.61087757687) " +
                "select inCloseProximity, proximityWith \n" +
                "insert into dataOut";

        long start = System.currentTimeMillis();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
        long end = System.currentTimeMillis();
        logger.info(String.format("Time to create ExecutionPlanRunTime: [%f sec]", ((end - start) / 1000f)));
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    Boolean proximity = (Boolean) event.getData(0);
                    Assert.assertEquals(expectedResult.get(eventCount), proximity);
                    String other = (String) event.getData(1);
                    Assert.assertEquals(expectedResultId.get(eventCount), other);
                    eventCount++;
                }
            }
        });
        executionPlanRuntime.start();
        generateEvents(executionPlanRuntime);
        Thread.sleep(1000);
        Assert.assertEquals(expectedResult.size(), eventCount);
    }
}