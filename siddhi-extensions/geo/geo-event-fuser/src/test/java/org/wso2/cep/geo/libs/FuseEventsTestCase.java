package org.wso2.cep.geo.libs;

import org.apache.log4j.Logger;
import org.junit.*;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class FuseEventsTestCase {
    private static Logger logger = Logger.getLogger(FuseEventsTestCase.class);
    private static List<String[]> data;

    protected static SiddhiManager siddhiManager;

    protected long start;
    protected long end;


    @Test
    public void testProcess() throws Exception {
        logger.info("TestProcess");
        String eventSubscribeExecutionPlan = "from dataIn#transform.geo:subscribeExecutionPlan() \n" +
                "select id, latitude, longitude, 1412236 as timeStamp, 12.6 as speed, 123.12 as heading, eventId, state, information\n" +
                "insert into dataOut;";

        start = System.currentTimeMillis();
        // Add multiple execution plans which are using the same input stream(dataIn) and outputting to same output stream(dataOut) ,allowing create same copy of the event coming from dataIn stream
        String queryReference1 = siddhiManager.addQuery(eventSubscribeExecutionPlan);

        String queryReference2 = siddhiManager.addQuery(eventSubscribeExecutionPlan);

        String queryReference3 = siddhiManager.addQuery(eventSubscribeExecutionPlan);


        String eventFuseExecutionPlan = "from dataOut#window.geo:eventsFunion(eventId) \n" +
                "select id, latitude, longitude, timeStamp, speed, heading, state , information, 'Testing' as notify\n" +
                "insert into dataFusedOut;";
        String eventFuseQueryReference = siddhiManager.addQuery(eventFuseExecutionPlan);

        end = System.currentTimeMillis();
        logger.info(String.format("Time to add query: [%f sec]", ((end - start) / 1000f)));
        siddhiManager.addCallback(eventFuseQueryReference, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                Integer eventsCount = 0;
                for (Event event : inEvents) {
                    eventsCount++;
                }
                Assert.assertEquals("1",eventsCount.toString());
            }
        });
        generateEvents();
    }


    private void generateEvents() throws Exception {
        InputHandler inputHandler = siddhiManager.getInputHandler("dataIn");
        for (String[] dataLine : data) {
            inputHandler.send(new Object[]{dataLine[0], dataLine[1], dataLine[2], dataLine[3], dataLine[4], dataLine[5]});
        }
    }

    @Before
    public void setUp() throws Exception {
        logger.info("Init Siddhi setUp");

        SiddhiConfiguration siddhiConfiguration = new SiddhiConfiguration();

        List<Class> extensions = new ArrayList<Class>(6);
        extensions.add(FuseEvents.class);
        extensions.add(ExecutionPlanSubscriber.class);

        siddhiConfiguration.setSiddhiExtensions(extensions);

        siddhiManager = new SiddhiManager(siddhiConfiguration);


        logger.info("calling setUpChild");
        siddhiManager.defineStream("define stream dataIn (id string, latitude double, longitude double, eventId string, state string, information string )");

        data = new ArrayList<String[]>();

        data.add(new String[]{"km-4354", "12.56", "56.32", UUID.randomUUID().toString(), "NORMAL", "NORMAL driving pattern"});
        data.add(new String[]{"km-4354", "12.56", "56.32", UUID.randomUUID().toString(), "NORMAL", "NOT NORMAL driving pattern"});
        data.add(new String[]{"km-4354", "12.56", "56.32", UUID.randomUUID().toString(), "NORMAL", "NOT NORMAL driving pattern"});
        data.add(new String[]{"km-4354", "12.56", "56.32", UUID.randomUUID().toString(), "NORMAL", "NORMAL driving pattern"});

    }

    @AfterClass
    public static void tearDown() throws Exception {
        Thread.sleep(1000);
        logger.info("Shutting down Siddhi");
        siddhiManager.shutdown();
    }
}