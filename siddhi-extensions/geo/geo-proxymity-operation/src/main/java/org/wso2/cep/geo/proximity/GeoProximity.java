package org.wso2.cep.geo.proximity;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@SiddhiExtension(namespace = "geo", function = "geoProximity")
public class GeoProximity extends FunctionExecutor {

    HashMap<String, String> closeSpatialObjects = new HashMap<String, String>();

    Map<String, Geometry> GeometryList;

    // ArrayList <String, Geometry> GList = new ArrayList<String, Geometry>();
    // List<List<Geometry>> listOfLists = new ArrayList<List<Geometry>>();

    public Attribute.Type getReturnType() {
        return Attribute.Type.STRING;
    }

    public void destroy() {
        // TODO Auto-generated method stub

    }

    @Override
    public void init(Type[] attributeTypes, SiddhiContext siddhiContext) {
        // TODO Auto-generated method stub
        // synchronized (this) {
        GeometryList = new HashMap<String, Geometry>();
        // }

    }

    @Override
    protected synchronized Object process(Object data) {

        ArrayList<String> IDList = new ArrayList<String>();
        Object functionParams[] = (Object[]) data;
        double proximityDist = Double.parseDouble(functionParams[0].toString());
        double pointOnelat = Double.parseDouble(functionParams[1].toString());
        double pointOnelong = Double.parseDouble(functionParams[2].toString());
        String currentId = functionParams[3].toString();
        double currentTime = Double.parseDouble(functionParams[4].toString());
        double giventime = Double.parseDouble(functionParams[5].toString()) * 1000; // Time take in UI front-end is seconds so here we convert it to milliseconds

        String pckttime = String.valueOf(currentTime);
        String previousId = null;
        double timediff;

        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
        // draw the buffer for each point
        Point currentPoint = geometryFactory.createPoint(new Coordinate(pointOnelat, pointOnelong));

        double bufferRadius = proximityDist / 110574.61087757687;// to convert
        // to degrees
        /*
        Computes a buffer area around this geometry having the given width.
		The buffer of a Geometry is the Minkowski sum or difference of the geometry with a disc of radius abs(distance).
		The buffer is constructed using 8 segments per quadrant to represent curves. The end cap style is CAP_ROUND
		*/
        Geometry buffer = currentPoint.buffer(bufferRadius);
        // if that id already exist update that entry

        GeometryList.put(currentId, buffer);
        // iterate
        // through the
        // list of all
        // available
        // vehicles

        for (Map.Entry<String, Geometry> entry : GeometryList.entrySet()) {
            previousId = entry.getKey().toString();
            String compositeKey = makeCompositeKey(currentId, previousId);
            Geometry bufferForPreviousId = (Geometry) entry.getValue(); // get the
            // buffer for
            // the current
            // position of
            // the vehicle

            if (!previousId.equalsIgnoreCase(currentId)) { // if the buffer is of
                // another vehicle
                if (currentPoint.within(bufferForPreviousId)) { // if the two vehicles
                    // are in close
                    // proximity

                    if (!closeSpatialObjects.containsKey(compositeKey)) {// check for how long //NOTE
                        // NEEDTO RESTRUCTURE!!!!!
                        // they have been close
                        closeSpatialObjects.put(compositeKey, pckttime);
                    }

                    double timecheck = Double.parseDouble(closeSpatialObjects.get(compositeKey));

                    timediff = currentTime - timecheck;

                    if (timediff >= giventime) { // if the time difference for
                        // being
                        // in close proximity is less
                        // than
                        // the user input time period,
                        // output
                        // true else false
                        IDList.add(previousId);
                    }
                } else {
                    if (closeSpatialObjects.containsKey(compositeKey)) {
                        closeSpatialObjects.remove(compositeKey);
                    }

                }
            }
        }

        // TODO Auto-generated method stub

        return generateOutput(IDList);
    }

    /**
     * generates the final output string *
     */
    public String generateOutput(ArrayList<String> myList) {

        String finalOutput = "false";
        String myString = "null";
        int i = 0;
        if (!myList.isEmpty()) {
            finalOutput = "true";
            for (i = 0; i < myList.size(); i++) {
                if (myString.equalsIgnoreCase("null")) {
                    myString = myList.get(i);
                } else {
                    myString = myString + "," + myList.get(i);
                }
            }

        }
        if (!myString.equalsIgnoreCase("null")) {
            finalOutput = finalOutput + "," + myString;
        }
        return finalOutput;
    }

    public String makeCompositeKey(String key1, String key2) {
        String compositeKey;
        if (key1.compareToIgnoreCase(key2) < 0) {
            compositeKey = key1 + key2;
        } else {
            compositeKey = key2 + key1;
        }
        return compositeKey;
    }

}
