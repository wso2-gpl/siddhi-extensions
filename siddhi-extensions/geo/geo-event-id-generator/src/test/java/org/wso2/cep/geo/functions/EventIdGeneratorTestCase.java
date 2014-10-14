package org.wso2.cep.geo.functions;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;

import java.util.ArrayList;
import java.util.List;

public abstract class EventIdGeneratorTestCase {

    private static Logger logger = Logger.getLogger(EventIdGenerator.class);

    protected static SiddhiManager siddhiManager;

    protected long start;
    protected long end;

    public abstract void setUpChild();

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("Init Siddhi");

        SiddhiConfiguration siddhiConfiguration = new SiddhiConfiguration();

        List<Class> extensions = new ArrayList<Class>(6);
        extensions.add(EventIdGenerator.class);

        siddhiConfiguration.setSiddhiExtensions(extensions);

        siddhiManager = new SiddhiManager(siddhiConfiguration);

    }

    @Before
    public void setUpChildren() {
        setUpChild();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        Thread.sleep(1000);
        logger.info("Shutting down Siddhi");
        siddhiManager.shutdown();
    }

}