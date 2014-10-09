package org.wso2.cep.geo.withinbuffer;

import java.util.ArrayList;
import java.util.List;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;



public class WithinBufferTest {
	
	public static void main(String[] args) throws Exception{
		
		//from anurudhdha
				SiddhiConfiguration conf = new SiddhiConfiguration();
				List<Class> classList = new ArrayList<Class>();
				classList.add(WithinBufferTransformer.class);
				conf.setSiddhiExtensions(classList);

				SiddhiManager siddhiManager = new SiddhiManager();
				siddhiManager.getSiddhiContext().setSiddhiExtensions(classList);

				siddhiManager
				.defineStream("define stream cseEventStream (id int, time double, lattitude double, longitude double, speed double ) ");
					//{'geometries':[{ 'type': 'Point', 'coordinates': [100.5, 0.5] },{ 'type': 'Point', 'coordinates': [100.5, 0.5] }]}
					//{"geometries":[{"type":"Point","coordinates":[  79.94248329162588,6.844997820293952]},{"type":"Point","coordinates":[100.0,0.0]}]}
				String queryReference = siddhiManager
				.addQuery("from " +
						"cseEventStream#transform.geo:withinbuffertransformer(\"{'features':[{ 'type': 'Feature', 'properties':{},'geometry':{'type': 'Point', 'coordinates': [  79.94248329162588,6.844997820293952] }}]}\") as tt "
				+ "insert into StockQuote;");

				siddhiManager.addCallback(queryReference, new QueryCallback() {
				@Override
				public void receive(long timeStamp, Event[] inEvents,
				Event[] removeEvents) {
				EventPrinter.print(timeStamp, inEvents, removeEvents);
				// Assert.assertTrue("IBM".equals(inEvents[0].getData(0)) ||
				// "WSO2".equals(inEvents[0].getData(0)));
				// count++;
				// eventArrived = true;
				}
				});
				
				//id,time,longitude,lat,speed, false as speedFlag
				InputHandler inputHandler = siddhiManager
				.getInputHandler("cseEventStream");
				//inputHandler.send(new Object[] { 1, 234.345,6.844997820293952,79.94248329162588,98.34});
				inputHandler.send(new Object[] { 1, 234.345,100.786, 6.9876,98.34});
				//inputHandler.send(new Object[] { "100.5", "100.5" });
				Thread.sleep(100);
				// Assert.assertEquals(3, count);
				// Assert.assertEquals("Event arrived", true, eventArrived);
				siddhiManager.shutdown();
				//gis:within(symbol1,symbol2)
	}

}
