/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.cep.geo.stationery;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vividsolutions.jts.geom.*;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.Collections;

/*Check whether a spatial object
* is in a defined stationery
* */
@SiddhiExtension(namespace = "geo", function = "withinstationery")
public class GeoInStationery extends FunctionExecutor {

    private GeometryFactory geometryFactory;
    private Object geometryType;
    private Map geoSyncMap;
    private static final double TO_DEGREE = 110574.61087757687;
    private static final String COORDINATES = "coordinates";
    private static final String POLYGON = "\"Polygon\"";
    private static final String CIRCLE = "\"Point\"";
    private static final String RADIUS = "radius";

    /**
     * Method will be called when initialising the custom function
     *
     * @param types
     * @param siddhiContext
     */
    public void init(Attribute.Type[] types, SiddhiContext siddhiContext) {

        Map<String, String> geoMap = new HashMap<String, String>();
        geoSyncMap= Collections.synchronizedMap(geoMap);

        if (types[0] != Attribute.Type.DOUBLE || types[1] != Attribute.Type.DOUBLE) {
            throw new QueryCreationException("lattitude and longitude must be provided as double values");
        }

        if (types[2] != Attribute.Type.STRING) {
            throw new QueryCreationException("shape parameter should be a geojson feature string");
        }

        String strGeometry = (String) attributeExpressionExecutors.get(2).execute(null);
        JsonObject jsonObject = new JsonParser().parse(strGeometry).getAsJsonObject();
        geometryFactory = JTSFactoryFinder.getGeometryFactory();


        //if the drawn shape is a polygon
        if (jsonObject.get("type").toString().equals(POLYGON)) {

            JsonArray jLocCoordinatesArray = (JsonArray) jsonObject.getAsJsonArray(COORDINATES).get(0);
            Coordinate[] coords = new Coordinate[jLocCoordinatesArray.size()];

            for (int i = 0; i < jLocCoordinatesArray.size(); i++) {
                JsonArray jArray = (JsonArray) jLocCoordinatesArray.get(i);
                coords[i] = new Coordinate(Double.parseDouble(jArray.get(0)
                        .toString()), Double.parseDouble(jArray.get(1).toString()));
            }

            LinearRing ring = geometryFactory.createLinearRing(coords);
            LinearRing holes[] = null; // use LinearRing[] to represent holes
            geometryType = geometryFactory.createPolygon(ring, holes);

        } else if (jsonObject.get("type").toString().equals(CIRCLE)) {

            JsonArray jLocCoordinatesArray = (JsonArray) jsonObject.getAsJsonArray(COORDINATES);
            Coordinate[] coords = new Coordinate[jLocCoordinatesArray.size()];

            for (int i = 0; i < jLocCoordinatesArray.size(); i++) {
                coords[i] = new Coordinate(Double.parseDouble(jLocCoordinatesArray.get(0)
                        .toString()), Double.parseDouble(jLocCoordinatesArray.get(1).toString()));
            }

            Point point = geometryFactory.createPoint(coords[0]); // create the points for GeoJSON file points
            double radius = Double.parseDouble(jsonObject.get(RADIUS).toString()) / TO_DEGREE; //convert to degrees

            geometryType = point.buffer(radius); //draw the buffer

        }
    }

    /**
     * Method called when sending events to process
     *
     * @param data
     * @return
     */
    protected Object process(Object data) {

        Object functionParams[] = (Object[]) data;
        double lattitude = (Double)functionParams[0];
        double longitude = (Double)functionParams[1];
        String currentId = functionParams[3].toString();
        double currentTime = Double.parseDouble(functionParams[4].toString());
        double givenTime = Double.parseDouble(functionParams[5].toString()) * 1000; // Time take in UI front-end is seconds so here we convert it to milliseconds
        double timeDiff;
        boolean withinStationery = false;
        boolean inStationery = false;

        //Creating a point
        Coordinate coord = new Coordinate(lattitude, longitude);
        Point currentPoint = geometryFactory.createPoint(coord);
        String pickedTime = String.valueOf(currentTime);

        if(geometryType instanceof Polygon){//if the drawn shape is a polygon
 
            if (currentPoint.within((Polygon) geometryType)) {
                withinStationery = true;
            }

        } else if (!(geometryType instanceof Polygon)) { //since logic has only two shapes. Should change if there are many

            if (currentPoint.within((Geometry) geometryType)) {
                withinStationery = true;
            }

        }

        if (withinStationery) {

            if (!geoSyncMap.containsKey(currentId)) {// if the object not already within the stationery

                geoSyncMap.put(currentId, pickedTime);
            }

            double previousTime = Double.parseDouble(geoSyncMap.get(currentId).toString());

            timeDiff = currentTime - previousTime;

            if (timeDiff >= givenTime) { // if the time deference is more than or equal to given time then generate the alert

                inStationery = true;
            }
        } else {

            if (geoSyncMap.containsKey(currentId)) {

                geoSyncMap.remove(currentId);
            }
        }

        return inStationery;
    }

    public void destroy() {

    }


    /**
     * Return type of the custom function mentioned
     *
     * @return
     */
    public Attribute.Type getReturnType() {

        return Attribute.Type.BOOL;
    }

}
