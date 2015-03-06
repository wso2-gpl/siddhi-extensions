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

public class RScriptTestCase extends RTransformTestCase {

	private int count;
	protected double doubleValue1;
	protected double doubleValue2;
	protected int intValue1;
	protected boolean boolValue1;

	@Before
	public void init() {
		count = 0;
	}

	@Test
	public void testRScript1() throws InterruptedException {
		log.info("R:runScript test1");
		SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);
		siddhiManager.defineStream(QueryFactory.createStreamDefinition().name("weather")
		                                       .attribute("time", Attribute.Type.LONG)
		                                       .attribute("temp", Attribute.Type.DOUBLE));

		String script = "c <- sum(time); m <- sum(temp) ;";

		Query query = QueryFactory.createQuery();
		query.from(QueryFactory.inputStream("weather")
		                       .transform("R", "runScript", Expression.value(script),
		                                  Expression.value("2"),
		                                  Expression.value("m double, c double")));
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
						doubleValue1 = (Double) event.getData1();
						doubleValue2 = (Double) event.getData0();
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
		Assert.assertEquals("Value 1 returned", (10 + 20) + 0.0, doubleValue1, 1e-4);
		Assert.assertEquals("Value 2 returned", (55.6 + 65.6), doubleValue2, 1e-4);
		siddhiManager.shutdown();
	}

	@Test
	public void testRScript2() throws InterruptedException {
		log.info("R:runScript test2");
		SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);
		siddhiManager.defineStream(QueryFactory.createStreamDefinition().name("weather")
		                                       .attribute("time", Attribute.Type.INT)
		                                       .attribute("temp", Attribute.Type.DOUBLE));
		String script = "\" " + "c <- sum(time); m <- sum(temp) ;\"";
		String query = "from weather#transform.R:runScript(" + script +
		                       ", \"2s\", \"c int, m double\") " + "select * " +
		                       "insert into weatherOutput";

		String queryReference = siddhiManager.addQuery(query);
		siddhiManager.addCallback(queryReference, new QueryCallback() {
			@Override
			public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				if (inEvents != null) {

					for (Event event : inEvents) {
						intValue1 = (Integer) event.getData0();
						doubleValue1 = (Double) event.getData1();
					}
					count++;
				}
			}
		});

			InputHandler inputHandler = siddhiManager.getInputHandler("weather");
		inputHandler.send(new Object[] { 10, 55.6 });
		inputHandler.send(new Object[] { 20, 65.6 });
		Thread.sleep(2500);
		inputHandler.send(new Object[] { 30, 75.6 });
		Thread.sleep(1000);
		Assert.assertEquals("Only one event must arrive", 1, count);
		Assert.assertEquals("Value 1 returned", 60, intValue1);
		Assert.assertEquals("Value 2 returned", (55.6 + 65.6 + 75.6), doubleValue1, 1e-4);
		siddhiManager.shutdown();
	}

	@Test
	public void testRScript3() throws InterruptedException {
		log.info("R:runScript test3");
		SiddhiManager siddhiManager = new SiddhiManager(siddhiConfiguration);
		siddhiManager.defineStream(QueryFactory.createStreamDefinition().name("weather")
		                                       .attribute("time", Attribute.Type.LONG)
		                                       .attribute("temp", Attribute.Type.DOUBLE));

		String script = "c <- sum(time); m <- sum(temp) ;";

		Query query = QueryFactory.createQuery();
		query.from(QueryFactory.inputStream("weather")
		                       .transform("R", "runScript", Expression.value(script),
		                                  Expression.value("1s"),
		                                  Expression.value("m double, c double")));
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
						doubleValue1 = (Double) event.getData1();
						doubleValue2 = (Double) event.getData0();
					}
					count++;
				}
			}
		});

		InputHandler inputHandler = siddhiManager.getInputHandler("weather");
		inputHandler.send(new Object[] { 10l, 55.6d });
		inputHandler.send(new Object[] { 20l, 65.6d });
		Thread.sleep(1500);
		inputHandler.send(new Object[] { 30l, 55.6d });
		inputHandler.send(new Object[] { 40l, 65.6d });
		Thread.sleep(1500);
		inputHandler.send(new Object[] { 50l, 75.6d });
		Thread.sleep(1000);

		Assert.assertEquals("Only two events must arrive", 2, count);
		Assert.assertEquals("Value 1 returned", (40 + 50) + 0.0, doubleValue1, 1e-4);
		Assert.assertEquals("Value 2 returned", (65.6 + 75.6), doubleValue2, 1e-4);
		siddhiManager.shutdown();
	}

}
