/*
 * Copyright (c) 2005 - 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.wso2.siddhi.gpl.extension.r;

import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.query.api.QueryFactory;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.query.Query;
import org.wso2.siddhi.query.api.query.output.stream.OutStream;

public class RSourceTestCase extends RTransformTestCase {

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
	public void testRScript1() throws InterruptedException {
		log.info("R:runScript test1");
		SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);
		siddhiManager.defineStream(QueryFactory.createStreamDefinition().name("weather")
		                                       .attribute("time", Attribute.Type.LONG)
		                                       .attribute("temp", Attribute.Type.DOUBLE));

		Query query = QueryFactory.createQuery();
		query.from(QueryFactory.inputStream("weather")
		                       .transform("R", "runSource",
		                                  Expression.value("src/test/resources/sample.R"),
		                                  Expression.value("1s"),
		                                  Expression.value("m double, c long")));
		query.select(QueryFactory.outputSelector().select("m", Expression.variable("m"))
		                         .select("c", Expression.variable("c")));
		query.insertInto("weatherOutput", OutStream.OutputEventsFor.ALL_EVENTS);

		String queryReference = siddhiManager.addQuery(query);
		siddhiManager.addCallback(queryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				if (inEvents != null) {
					for (Event event : inEvents) {
						value1 = (Double) event.getData0();
						valueLong = (Long) event.getData1();
					}
					count++;
				}
			}
		});

		InputHandler inputHandler = siddhiManager.getInputHandler("weather");
		inputHandler.send(new Object[] { 10l, 55.6d });
		Thread.sleep(2000);
		inputHandler.send(new Object[] { 20l, 65.6d });
		Thread.sleep(1000);
		Assert.assertEquals("Only one event must arrive", 1, count);
		Assert.assertEquals("Value 1 returned", 121.2, value1, 1e-4);
		Assert.assertEquals("Value 2 returned", 30l, valueLong, 1e-4);
		siddhiManager.shutdown();
	}

	// get integer, float values to the output stream
	@Test
	public void testRScript2() throws InterruptedException {
		log.info("R:runScript test2");
		SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);
		siddhiManager.defineStream(QueryFactory.createStreamDefinition().name("weather")
		                                       .attribute("time", Attribute.Type.LONG)
		                                       .attribute("temp", Attribute.Type.DOUBLE));

		Query query = QueryFactory.createQuery();
		query.from(QueryFactory.inputStream("weather")
		                       .transform("R", "runSource",
		                                  Expression.value("src/test/resources/sample2.R"),
		                                  Expression.value("2"), Expression.value("m int, c float")));
		query.select(QueryFactory.outputSelector().select("m", Expression.variable("m"))
		                         .select("c", Expression.variable("c")));
		query.insertInto("weatherOutput", OutStream.OutputEventsFor.ALL_EVENTS);

		String queryReference = siddhiManager.addQuery(query);
		siddhiManager.addCallback(queryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				if (inEvents != null) {

					for (Event event : inEvents) {
						value1 = (Integer) event.getData0();
						valueFloat = (Float) event.getData1();
					}
					count++;
				}
			}
		});

		InputHandler inputHandler = siddhiManager.getInputHandler("weather");
		inputHandler.send(new Object[] { 10l, 55.6d });
		inputHandler.send(new Object[] { 20l, 65.6d });
		inputHandler.send(new Object[] { 30l, 75.6d });
		Thread.sleep(1000);
		Assert.assertEquals("Only one event must arrive", 1, count);
		Assert.assertEquals("Value 1 returned", 121, value1, 1e-4);
		Assert.assertEquals("Value 2 returned", 30f, valueFloat);
		siddhiManager.shutdown();
	}

	// get string, bool to the output stream
	@Test
	public void testRScript3() throws InterruptedException {
		log.info("R:runScript test2");
		SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);
		siddhiManager.defineStream(QueryFactory.createStreamDefinition().name("weather")
		                                       .attribute("time", Attribute.Type.LONG)
		                                       .attribute("temp", Attribute.Type.DOUBLE));

		Query query = QueryFactory.createQuery();
		query.from(QueryFactory.inputStream("weather")
		                       .transform("R", "runSource",
		                                  Expression.value("src/test/resources/sample3.R"),
		                                  Expression.value("2"),
		                                  Expression.value("c string, m bool")));
		query.select(QueryFactory.outputSelector().select("c", Expression.variable("c"))
		                         .select("m", Expression.variable("m")));
		query.insertInto("weatherOutput", OutStream.OutputEventsFor.ALL_EVENTS);

		String queryReference = siddhiManager.addQuery(query);
		siddhiManager.addCallback(queryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				if (inEvents != null) {

					for (Event event : inEvents) {
						valueString = event.getData0().toString();
						valueBool = (Boolean) event.getData1();
					}
					count++;
				}
			}
		});

		InputHandler inputHandler = siddhiManager.getInputHandler("weather");
		inputHandler.send(new Object[] { 123l, 55.6d });
		inputHandler.send(new Object[] { 101l, 72.3d });
		Thread.sleep(1000);
		Assert.assertEquals("Only one event must arrive", 1, count);
		Assert.assertEquals("Value 1 returned", "178.6", valueString);
		Assert.assertEquals("Value 2 returned", true, valueBool);
		siddhiManager.shutdown();
	}

}
