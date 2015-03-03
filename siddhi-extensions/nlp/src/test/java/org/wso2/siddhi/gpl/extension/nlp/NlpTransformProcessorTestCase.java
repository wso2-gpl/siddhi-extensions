package org.wso2.siddhi.gpl.extension.nlp;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by malithi on 9/10/14.
 */
public abstract class NlpTransformProcessorTestCase {

    private static Logger logger = Logger.getLogger(NlpTransformProcessorTestCase.class);
    protected static List<String[]> data;
    protected static SiddhiManager siddhiManager;

    protected long start;
    protected long end;
    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("Init Siddhi");
        siddhiManager = new SiddhiManager();
    }

    protected void generateEvents(ExecutionPlanRuntime executionPlanRuntime, String dataIn) throws Exception {
        InputHandler inputHandler = executionPlanRuntime.getInputHandler(dataIn);
        for (Object[] dataLine : data) {
            inputHandler.send(dataLine);
        }
    }

    protected void assertOutput(List<Event> outputEvents, String[] expectedMatches, int[] inStreamIndices) {
        for (int i = 0; i < outputEvents.size(); i++) {
            Event event = outputEvents.get(i);
            //Compare expected output stream match and received match
            assertEquals(expectedMatches[i], event.getData(0));
            //Compare expected output stream username and received username
            assertEquals(data.get(inStreamIndices[i])[0], event.getData(1));
            //Compare expected output stream text and received text
            assertEquals(data.get(inStreamIndices[i])[1], event.getData(2));
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        Thread.sleep(1000);
    }

    protected List<Event> runQuery(String query, String queryName, String dataInStreamName) throws Exception {
        start = System.currentTimeMillis();
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(query);
        end = System.currentTimeMillis();
        logger.info(String.format("Time to add query: [%f sec]", ((end - start) / 1000f)));

        final List<Event> eventList = new ArrayList<Event>();

        executionPlanRuntime.addCallback(queryName, new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event event : inEvents) {
                    eventList.add(event);
                }
            }
        });
        executionPlanRuntime.start();
        generateEvents(executionPlanRuntime, dataInStreamName);

        return eventList;
    }

}
