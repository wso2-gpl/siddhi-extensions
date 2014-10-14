package org.wso2.cep.geo.withinbuffer;


import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.log4j.Logger;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.event.in.InEvent;
import org.wso2.siddhi.core.event.in.InListEvent;
import org.wso2.siddhi.core.event.in.InStream;
import org.wso2.siddhi.core.executor.expression.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.transform.TransformProcessor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@SiddhiExtension(namespace = "geo", function = "withinbuffertransformer")
public class WithinBufferTransformer extends TransformProcessor {

    public Geometry[] mybufferList;

    //public JsonArray jLocCoordinatesArray;
    public JsonArray jmLocCoordinatesArray;
    Logger log = Logger.getLogger(WithinBufferTransformer.class);
    HashMap<String, String[]> test = new HashMap<String, String[]>();

    double giventime = 10.0;
    double myRadius = 1.0;


    int pointLength;


    public void destroy() {
        // TODO Auto-generated method stub

    }

    @Override
    protected InStream processEvent(InEvent event) {
        //id,time,longitude,lat,speed, false as speedFlag
        String withinPoint = "null";
        Boolean withinState = false;
        Boolean withinTime = false;


        int id = Integer.parseInt(event.getData0().toString());
        String strid = event.getData0().toString();
        double time = Double.parseDouble(event.getData1().toString());
        String strtime = event.getData1().toString();
        double lat = Double.parseDouble(event.getData3().toString());
        double longitude = Double.parseDouble(event.getData2().toString());
        double speed = Double.parseDouble(event.getData4().toString());

        double timediff;


        int x = 0;


        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();


        Coordinate checkpoint = new Coordinate(lat, longitude);
        Point cpoint = geometryFactory.createPoint(checkpoint);


        for (x = 0; x < pointLength; x++) { //for each of the buffers drawn

            Geometry myBuffer = mybufferList[x];
            if (cpoint.within(myBuffer)) {    //check whether the point is within
                withinState = true;

                JsonObject jmObject = (JsonObject) jmLocCoordinatesArray.get(x);
                JsonObject mgeometryObject = jmObject.getAsJsonObject("geometry");
                JsonArray coordArray = mgeometryObject.getAsJsonArray("coordinates");
                withinPoint = coordArray.get(0).toString() + "," + coordArray.get(1).toString();// the point for which the stationary condition became true

                //put it into Hashmap as the latest within true event for the id if hashmap doesnt contain that id already
                if (test.containsKey(strid) == false || test.containsKey(strid) == true && test.get(strid)[1].equalsIgnoreCase(myBuffer.toString()) == false) {
                    String[] myarray = {strtime, myBuffer.toString()};
                    test.put(strid, myarray);
                } else {
                    timediff = time - Double.parseDouble(test.get(strid)[0]);
                    //test.remove(strid);
                    if (timediff >= giventime) {
                        withinTime = true;
                    }
                }
            } else {
                if (test.containsKey(strid) == true) {
                    test.remove(strid);
                }
            }


            //on false check if a true exists in the hashmap already and if so get the time difference and remove the one from the hashmap
            //, otherwise do nothing
        }


        Object[] data = new Object[]{
                id,
                time,
                lat,
                longitude,
                speed,
                false,
                withinState,
                withinPoint,
                withinTime
        };


        return new InEvent(event.getStreamId(), System.currentTimeMillis(), data);

    }

    @Override
    protected InStream processEvent(InListEvent listEvent) {


        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Object[] currentState() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void restoreState(Object[] data) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void init(Expression[] parameters, List<ExpressionExecutor> expressionExecutors,
                        StreamDefinition inStreamDefinition, StreamDefinition outStreamDefinition,
                        String elementId, SiddhiContext siddhiContext) {

        ArrayList<Object> paramList = new ArrayList<Object>();
        for (int i = 0, size = expressionExecutors.size(); i < size; i++) {
            paramList.add(expressionExecutors.get(i).execute(null));
        }//[{'features':[{ 'type': 'Feature', 'properties':{},'geometry':{'type': 'Point', 'coordinates': [  79.94248329162588,6.844997820293952] }},{ 'type': 'Feature', 'properties':{},'geometry':{'type': 'Point', 'coordinates': [  79.94248329162588,6.844997820293952] }}]}]

        //creating the bufferpoints
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
        String mystring = paramList.get(0).toString();

        if (paramList.get(1) != null) {

            giventime = (Double) paramList.get(1);
            myRadius = (Double) paramList.get(2) / 110574.61087757687; //to convert into latitudes since the calculations are done in geo space

        }

        JsonElement mcoordinateArray = new JsonParser().parse(mystring);
        JsonObject jmObject = mcoordinateArray.getAsJsonObject();

        jmLocCoordinatesArray = jmObject.getAsJsonArray("features");
        pointLength = jmLocCoordinatesArray.size();
        mybufferList = new Geometry[jmLocCoordinatesArray.size()];

        for (int i = 0; i < pointLength; i++) {

            //getting the geometry feature

            JsonObject jObject = (JsonObject) jmLocCoordinatesArray.get(i);

            JsonObject geometryObject = jObject.getAsJsonObject("geometry");


            JsonArray coordArray = geometryObject.getAsJsonArray("coordinates");

            double lattitude = Double.parseDouble(coordArray.get(0).toString());
            double longitude = Double.parseDouble(coordArray.get(1).toString());
            //inserting for passing to UI


            Coordinate coord = new Coordinate(lattitude, longitude);
            Point point = geometryFactory.createPoint(coord); // crewate the points for GeoJSON file points

            Geometry buffer = point.buffer(myRadius); //draw the buffer
            mybufferList[i] = buffer; // put it into the list
        }


        //defining the output Stream

        this.outStreamDefinition = new StreamDefinition().name("outputStream")
                .attribute("id", Attribute.Type.INT)
                .attribute("time", Attribute.Type.DOUBLE)
                .attribute("longitude", Attribute.Type.DOUBLE)
                .attribute("lat", Attribute.Type.DOUBLE)
                .attribute("speed", Attribute.Type.DOUBLE)
                .attribute("speedFlag", Attribute.Type.BOOL)
                .attribute("withinFlag", Attribute.Type.BOOL) //if the device is within the buffer
                .attribute("withinPoint", Attribute.Type.STRING)//the point 
                .attribute("withinTime", Attribute.Type.BOOL);


        // TODO Auto-generated method stub

    }

}
