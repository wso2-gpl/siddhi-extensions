package org.wso2.cep.geo;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class GeoIsWithinTestCase {
    protected static SiddhiManager siddhiManager;
    private static Logger logger = Logger.getLogger(GeoIsWithinTestCase.class);
    private static List<String[]> data;
    private static List<Boolean> expectedOutput;
    protected long start;
    protected long end;
    int eventNumber = 0;

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("Init Siddhi setUp");

        SiddhiConfiguration siddhiConfiguration = new SiddhiConfiguration();

        List<Class> extensions = new ArrayList<Class>(1);
        extensions.add(GeoIsWithin.class);
        siddhiConfiguration.setSiddhiExtensions(extensions);
        siddhiManager = new SiddhiManager(siddhiConfiguration);

        siddhiManager.defineStream("define stream dataIn (id string, latitude double, longitude double, eventId string, state string, information string )");

        data = new ArrayList<String[]>();
        expectedOutput = new ArrayList<Boolean>();

        data.add(new String[]{"km-4354", "6.9270786", "79.861243", UUID.randomUUID().toString(), "NORMAL", "NORMAL driving pattern"});
        expectedOutput.add(true);
        data.add(new String[]{"km-4354", "6.91049338", "79.85399723", UUID.randomUUID().toString(), "NORMAL", "NOT NORMAL driving pattern"});
        expectedOutput.add(false);
        data.add(new String[]{"km-4354", "38.54816542", "-118.19091797", UUID.randomUUID().toString(), "NORMAL", "NORMAL driving pattern"});
        expectedOutput.add(false);
        data.add(new String[]{"km-4354", "6.93017582", "79.8625803", UUID.randomUUID().toString(), "NORMAL", "NOT NORMAL driving pattern"});
        expectedOutput.add(true);
        data.add(new String[]{"km-4354", "33.58402124", "-80.81010818", UUID.randomUUID().toString(), "NORMAL", "NORMAL driving pattern"});
        expectedOutput.add(false);
        data.add(new String[]{"km-4354", "6.92455235", "79.86094952", UUID.randomUUID().toString(), "NORMAL", "NORMAL driving pattern"});
        expectedOutput.add(true);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        Thread.sleep(1000);
        logger.info("Shutting down Siddhi");
        siddhiManager.shutdown();
    }

    @Test
    public void testProcess() throws Exception {
        logger.info("Starting testProcess");
        String withinCheckExecutionPlan = "from dataIn \n" +
                "select id, latitude, longitude, eventId, state, information, \n" +
                "geo:iswithin(longitude,latitude,\"{'type':'Polygon','coordinates':[[[79.85395431518555,6.915009335274164],[79.85395431518555,6.941081755563143],[79.88382339477539,6.941081755563143],[79.88382339477539,6.915009335274164],[79.85395431518555,6.915009335274164]]]}\") as isWithin \n" +
                "insert into dataOut;";

        start = System.currentTimeMillis();
        String withinCheckExecutionPlanReference = siddhiManager.addQuery(withinCheckExecutionPlan);
        end = System.currentTimeMillis();

        logger.info(String.format("Time to add query: [%f sec]", ((end - start) / 1000f)));
        siddhiManager.addCallback(withinCheckExecutionPlanReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    Boolean isWithin = (Boolean) event.getData(6);
                    Assert.assertEquals(expectedOutput.get(eventNumber++), isWithin);
                }
            }
        });
        generateEvents();
    }

    private void generateEvents() throws Exception {
        InputHandler inputHandler = siddhiManager.getInputHandler("dataIn");
        for (String[] dataLine : data) {
            inputHandler.send(new Object[]{dataLine[0], Double.valueOf(dataLine[1]), Double.valueOf(dataLine[2]), dataLine[3], dataLine[4], dataLine[5]});
        }
    }
}