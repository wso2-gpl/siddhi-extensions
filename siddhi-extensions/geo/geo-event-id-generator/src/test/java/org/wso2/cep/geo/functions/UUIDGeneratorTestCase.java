package org.wso2.cep.geo.functions;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class UUIDGeneratorTestCase extends EventIdGeneratorTestCase {
    private static Logger logger = org.apache.log4j.Logger.getLogger(UUIDGeneratorTestCase.class);
    private static List<String[]> data;
    private static int eventCount = 0;

    @Test
    public void testProcess() throws Exception {
        logger.info("TestProcess");

        String query = "from dataIn \n" +
                "select id, latitude, longitude, speed, geo:generateEventId() as eventId\n" +
                "insert into dataOut;";
        start = System.currentTimeMillis();
        String queryReference = siddhiManager.addQuery(query);
        end = System.currentTimeMillis();
        logger.info(String.format("Time to add query: [%f sec]", ((end - start) / 1000f)));
        siddhiManager.addCallback(queryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    String uuid = (String) event.getData(4);
                    logger.info("UUID = "+uuid);
                    Assert.assertEquals(true,isUUID(uuid));
                }
            }
        });
        generateEvents();
    }

    @Override
    public void setUpChild() {
        siddhiManager.defineStream("define stream dataIn (id string, latitude double, longitude double, speed float)");
    }

    @BeforeClass
    public static void loadData() throws Exception {
        data = new ArrayList<String[]>();

        data.add(new String[]{"km-4354", "8.641987", "79.35022", "32.21"});
        data.add(new String[]{"km-4354", "8.549587", "79.36022", "12.21"});
        data.add(new String[]{"km-4354", "8.569867", "79.37022", "56.21"});
        data.add(new String[]{"km-4354", "8.564879", "79.33022", "29.21"});
    }

    private void generateEvents() throws Exception {
        InputHandler inputHandler = siddhiManager.getInputHandler("dataIn");
        for (String[] dataLine : data) {
            inputHandler.send(new Object[]{dataLine[0], dataLine[1], dataLine[2], dataLine[3]});
        }
    }

    public boolean isUUID(String string) {
        try {
            UUID.fromString(string);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}