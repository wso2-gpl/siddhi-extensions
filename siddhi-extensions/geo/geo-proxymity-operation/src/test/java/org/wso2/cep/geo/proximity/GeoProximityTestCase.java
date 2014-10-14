package org.wso2.cep.geo.proximity;

import java.util.ArrayList;
import java.util.List;

import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;



public class GeoProximityTestCase {
public static void main(String[] args) throws Exception{
		
		//from anurudhdha
				SiddhiConfiguration conf = new SiddhiConfiguration();
				List<Class> classList = new ArrayList<Class>();
				classList.add(GeoProximity.class);
				conf.setSiddhiExtensions(classList);

				SiddhiManager siddhiManager = new SiddhiManager();
				siddhiManager.getSiddhiContext().setSiddhiExtensions(classList);

				/*siddhiManager
				.defineStream("define stream cseEventStream ( id int , time double, longitude double,lat double ,speed double , tableLat double, tableLong double, tableId int) ");
					//{'geometries':[{ 'type': 'Point', 'coordinates': [100.5, 0.5] },{ 'type': 'Point', 'coordinates': [100.5, 0.5] }]}
					//{"geometries":[{"type":"Point","coordinates":[  79.94248329162588,6.844997820293952]},{"type":"Point","coordinates":[100.0,0.0]}]}
				String queryReference = siddhiManager
				.addQuery("from " +
						"mygeo:geoproximity( id, time , lattitude , longitude , speed,tableLat , tableLong , tableId  ) as tt "
				+ "insert into StockQuote;");*/
				siddhiManager
				.defineStream("define stream cseEventStream ( id int , time double, longitude double,lat double) ");
					//{'geometries':[{ 'type': 'Point', 'coordinates': [100.5, 0.5] },{ 'type': 'Point', 'coordinates': [100.5, 0.5] }]}
					//{"geometries":[{"type":"Point","coordinates":[  79.94248329162588,6.844997820293952]},{"type":"Point","coordinates":[100.0,0.0]}]}
				
				String queryReference = siddhiManager
						.addQuery("from cseEventStream "
						+ "select id, time, geo:geoProximity(1,lat,longitude,id,time,1 ) as tt "
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
				inputHandler.send(new Object[] { 1, 234.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 2, 244.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 3, 244.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 1, 254.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 2, 254.345,5, 100});
				inputHandler.send(new Object[] { 3, 254.345,5, 100});
				inputHandler.send(new Object[] { 1, 264.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 4, 244.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 5, 254.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 4, 254.345,5, 100});
				inputHandler.send(new Object[] { 4, 254.345,5, 100});
				inputHandler.send(new Object[] { 1, 264.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 2, 274.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 1, 284.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 5, 264.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 5, 274.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 5, 284.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 2, 284.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 5, 294.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 5, 234.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 2, 244.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 3, 244.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 1, 254.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 5, 254.345,5, 100});
				inputHandler.send(new Object[] { 3, 254.345,5, 100});
				inputHandler.send(new Object[] { 1, 264.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 2, 274.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 1, 284.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 3, 264.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 3, 274.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 3, 284.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 2, 284.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 1, 294.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 1, 234.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 2, 244.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 3, 244.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 1, 254.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 2, 254.345,5, 100});
				inputHandler.send(new Object[] { 3, 254.345,5, 100});
				inputHandler.send(new Object[] { 1, 264.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 2, 274.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 1, 284.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 3, 264.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 3, 274.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 3, 284.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 2, 284.345,100.786, 6.9876});
				inputHandler.send(new Object[] { 1, 294.345,100.786, 6.9876});
				
				
				//inputHandler.send(new Object[] { "100.5", "100.5" });
				Thread.sleep(100);
				// Assert.assertEquals(3, count);
				// Assert.assertEquals("Event arrived", true, eventArrived);
				siddhiManager.shutdown();
				//gis:within(symbol1,symbol2)
	}


}
