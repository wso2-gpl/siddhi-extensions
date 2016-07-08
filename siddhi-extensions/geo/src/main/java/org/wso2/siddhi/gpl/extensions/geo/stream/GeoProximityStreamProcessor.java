/*
 * Copyright (C) 2015 WSO2 Inc. (http://wso2.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.wso2.siddhi.gpl.extensions.geo.stream;

import com.vividsolutions.jts.geom.Geometry;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.gpl.extensions.geo.internal.util.GeoOperation;
import org.wso2.siddhi.gpl.extensions.geo.internal.util.WithinDistanceOperation;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class GeoProximityStreamProcessor extends StreamProcessor {

    private GeoOperation geoOperation;
    private double radius;
    private ConcurrentHashMap<String, Geometry> map = new ConcurrentHashMap<String, Geometry>();
    private Set<String> set = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /**
     * The init method of the StreamProcessor, this method will be called before other methods
     *
     * @param inputDefinition              the incoming stream definition
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param executionPlanContext         the context of the execution plan
     * @return the additional output attributes introduced by the function
     */
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        this.geoOperation = new WithinDistanceOperation();
        this.geoOperation.init(attributeExpressionExecutors, 1, attributeExpressionLength - 1);
        if (attributeExpressionExecutors[attributeExpressionLength - 1].getReturnType() != Type.DOUBLE) {
            throw new ExecutionPlanCreationException("Last parameter should be a double");
        }
        radius = (Double) attributeExpressionExecutors[attributeExpressionLength - 1].execute(null);
        ArrayList<Attribute> attributeList = new ArrayList<Attribute>();
        attributeList.add(new Attribute("proximityWith", Type.STRING));
        attributeList.add(new Attribute("inCloseProximity", Type.BOOL));
        return attributeList;
    }

    /**
     * This will be called only once, to acquire required resources
     * after initializing the system and before processing the events.
     */
    @Override
    public void start() {

    }

    /**
     * This will be called only once, to release the acquired resources
     * before shutting down the system.
     */
    @Override
    public void stop() {

    }

    /**
     * The serializable state of the element, that need to be
     * persisted for the reconstructing the element to the same state
     * on a different point of time
     *
     * @return stateful objects of the element as an array
     */
    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    /**
     * The serialized state of the element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the element as an array on
     *              the same order provided by currentState().
     */
    @Override
    public void restoreState(Object[] state) {

    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk      the event chunk that need to be processed
     * @param nextProcessor         the next processor to which the success events need to be passed
     * @param streamEventCloner     helps to clone the incoming event for local storage or modification
     * @param complexEventPopulater helps to populate the events with the resultant attributes
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        while (streamEventChunk.hasNext()) {
            StreamEvent streamEvent = streamEventChunk.next();
            Geometry currentGeometry, previousGeometry;
            Object[] data = new Object[attributeExpressionLength - 1];
            for (int i = 1; i < attributeExpressionLength; i++) {
                data[i - 1] = attributeExpressionExecutors[i].execute(streamEvent);
            }
            String currentId = attributeExpressionExecutors[0].execute(streamEvent).toString();
            String previousId;
            currentGeometry = geoOperation.getCurrentGeometry(data);
            if(!map.contains(currentId)) {
                map.put(currentId, currentGeometry);
            }
            for (Map.Entry<String, Geometry> entry : map.entrySet()) {
                previousId = entry.getKey();
                if (!previousId.equals(currentId)) {
                    previousGeometry = entry.getValue();
                    boolean within = (Boolean) geoOperation.operation(currentGeometry, previousGeometry, new Object[]{radius});
                    String key = makeCompositeKey(currentId, previousId);
                    boolean contains = set.contains(key);
                    if (contains) {
                        if (!within) {
                            //alert out
                            StreamEvent newStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                            complexEventPopulater.populateComplexEvent(newStreamEvent, new Object[]{previousId, within});
                            streamEventChunk.insertBeforeCurrent(newStreamEvent);
                            set.remove(key);
                        }
                    } else {
                        if (within) {
                            //alert in
                            StreamEvent newStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                            complexEventPopulater.populateComplexEvent(newStreamEvent, new Object[]{previousId, within});
                            streamEventChunk.insertBeforeCurrent(newStreamEvent);
                            set.add(key);
                        }
                    }
                }
            }
            streamEventChunk.remove();
        }
        nextProcessor.process(streamEventChunk);
    }
/*
    private Object[] toOutput(Geometry geometry, boolean within) {
        if (geoOperation.point) {
            return new Object[]{((Point) geometry).getX(), ((Point) geometry).getY(), within};
        } else {
            return new Object[]{GeometryUtils.geometrytoJSON(geometry), within};
        }
    }*/
    public String makeCompositeKey(String key1, String key2) {
        if (key1.compareToIgnoreCase(key2) < 0) {
            return key1 + "~" + key2;
        } else {
            return key2 + "~" + key1;
        }
    }
}
