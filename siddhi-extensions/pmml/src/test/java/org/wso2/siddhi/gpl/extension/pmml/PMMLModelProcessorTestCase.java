/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.siddhi.gpl.extension.pmml;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

public class PMMLModelProcessorTestCase {

    private volatile boolean eventArrived;

    @Before
    public void init() {
        eventArrived = false;
    }

    @Test
    public void predictFunctionTest() throws InterruptedException, URISyntaxException {

        URL resource = PMMLModelProcessorTestCase.class.getResource("/decision-tree.pmml");
        String pmmlFile = new File(resource.toURI()).getAbsolutePath();

        SiddhiManager siddhiManager = new SiddhiManager();

        String inputStream = "define stream InputStream " +
                "(root_shell double, su_attempted double, num_root double, num_file_creations double, num_shells double, num_access_files double, num_outbound_cmds double, is_host_login double, is_guest_login double, count double, srv_count double, serror_rate double, srv_serror_rate double);";

        String query = "@info(name = 'query1') " +
                "from InputStream#pmml:predict('"+ pmmlFile +"') " +
                "select * " +
                "insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (inEvents != null) {
                    Assert.assertEquals(0.018258426966292134, inEvents[0].getData(15));
                }
            }

        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{6, 148, 72, 35, 0, 33.6, 0.627, 50, 1, 2, 3, 4, 5});
        Thread.sleep(1000);
        junit.framework.Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

//    @Test
//    public void predictFunctionWithSelectedAttributesTest() throws InterruptedException, URISyntaxException {
//
//        URL resource = PMMLModelProcessorTestCase.class.getResource("/decision-tree.pmml");
//        String modelStorageLocation = new File(resource.toURI()).getAbsolutePath();
//
//        SiddhiManager siddhiManager = new SiddhiManager();
//
//        String inputStream = "define stream InputStream " +
//                "(response double, root_shell double, su_attempted double, num_root double, num_file_creations double, num_shells double, num_access_files double, num_outbound_cmds double, is_host_login double, is_guest_login double, count double, srv_count double, serror_rate double, srv_serror_rate double);";
//
//        String query = "@info(name = 'query1') " +
//                "from InputStream#pmml:predict('" + modelStorageLocation + "', NumPregnancies, PG2, DBP, TSFT, SI2, BMI, DPF, Age) " +
//                "select prediction " +
//                "insert into outputStream ;";
//
//        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inputStream + query);
//
//        executionPlanRuntime.addCallback("query1", new QueryCallback() {
//            @Override
//            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
//                EventPrinter.print(timeStamp, inEvents, removeEvents);
//                eventArrived = true;
//                if (inEvents != null) {
//                    Assert.assertEquals(0.9176214029655854, inEvents[0].getData(0));
//                }
//            }
//
//        });
//
//        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
//        executionPlanRuntime.start();
//        inputHandler.send(new Object[]{6, 148, 72, 35, 0, 33.6, 0.627, 50});
//        Thread.sleep(1000);
//        junit.framework.Assert.assertTrue(eventArrived);
//        executionPlanRuntime.shutdown();
//    }
}
