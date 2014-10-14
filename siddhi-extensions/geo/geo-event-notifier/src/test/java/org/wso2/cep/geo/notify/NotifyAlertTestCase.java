package org.wso2.cep.geo.notify;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.ArrayList;
import java.util.List;

public class NotifyAlertTestCase extends GeoNotifyTestCase {
    private static Logger logger = org.apache.log4j.Logger.getLogger(NotifyAlertTestCase.class);
    private static List<String[]> data;
    private static List<Boolean> expectedResult;
    private static int eventCount = 0;

    @Test
    public void testProcess() throws Exception {
        logger.info("TestProcess");
        String query = "from dataIn \n" +
                "select id ,latitude ,longitude ,eventId ,state ,information , geo:needToNotify(id,information) as notify\n" +
                "insert into dataOut;";
        start = System.currentTimeMillis();
        String queryReference = siddhiManager.addQuery(query);
        end = System.currentTimeMillis();
        logger.info(String.format("Time to add query: [%f sec]", ((end - start) / 1000f)));
        siddhiManager.addCallback(queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    Boolean notify = (Boolean) event.getData(6);
                    Assert.assertEquals(expectedResult.get(eventCount++), notify);
                }
            }
        });
        generateEvents();
    }

    @Override
    public void setUpChild() {
        siddhiManager.defineStream("define stream dataIn (id string, latitude double, longitude double, eventId string, state string, information string )");
    }

    @BeforeClass
    public static void loadData() throws Exception {
        data = new ArrayList<String[]>();
        expectedResult = new ArrayList<Boolean>();

        data.add(new String[]{"km-4354", "12.56", "56.32", "12345684", "NORMAL", "NORMAL driving pattern"});
        expectedResult.add(false);

        data.add(new String[]{"km-4354", "12.56", "56.32", "12345684", "NORMAL", "NOT NORMAL driving pattern"});
        expectedResult.add(true);

        data.add(new String[]{"km-4354", "12.56", "56.32", "12345684", "NORMAL", "NOT NORMAL driving pattern"});
        expectedResult.add(false);

        data.add(new String[]{"km-4354", "12.56", "56.32", "12345684", "NORMAL", "NORMAL driving pattern"});
        expectedResult.add(true);
    }

    private void generateEvents() throws Exception {
        InputHandler inputHandler = siddhiManager.getInputHandler("dataIn");
        for (String[] dataLine : data) {
            inputHandler.send(new Object[]{dataLine[0], dataLine[1], dataLine[2], dataLine[3], dataLine[4], dataLine[5]});
        }
    }
}