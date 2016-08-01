/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.gpl.siddhi.extension.r;

import org.apache.log4j.Logger;
import org.rosuda.REngine.JRI.JRIEngine;
import org.rosuda.REngine.*;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.compiler.SiddhiCompiler;
import org.wso2.siddhi.query.compiler.exception.SiddhiParserException;

import java.util.ArrayList;
import java.util.List;

public abstract class RStreamProcessor extends StreamProcessor {

    List<Attribute> inputAttributes = new ArrayList<Attribute>();

    REXP outputs;
    REXP script;
    REXP env;

    REngine re;
    static Logger log = Logger.getLogger(RStreamProcessor.class);

    @Override
    protected void process(ComplexEventChunk<StreamEvent> complexEventChunk, Processor processor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        StreamEvent streamEvent;
        StreamEvent lastCurrentEvent = null;
        List<StreamEvent> eventList = new ArrayList<StreamEvent>();
        while (complexEventChunk.hasNext()) {
            streamEvent = complexEventChunk.next();
            if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                eventList.add(streamEvent);
                lastCurrentEvent = streamEvent;
                complexEventChunk.remove();
            }
        }
        if(!eventList.isEmpty()) {
            complexEventPopulater.populateComplexEvent(lastCurrentEvent, process(eventList));
            complexEventChunk.add(lastCurrentEvent);
        }
        nextProcessor.process(complexEventChunk);
    }

    private Object[] process(List<StreamEvent> eventList) {
        try {
            REXP eventData;
            ExpressionExecutor expressionExecutor;
            for (int j = 2; j < attributeExpressionLength; j++) {
                expressionExecutor = attributeExpressionExecutors[j];
                switch (expressionExecutor.getReturnType()) {
                    case DOUBLE:
                        eventData = doubleToREXP(eventList, expressionExecutor);
                        break;
                    case FLOAT:
                        eventData = floatToREXP(eventList, expressionExecutor);
                        break;
                    case INT:
                        eventData = intToREXP(eventList, expressionExecutor);
                        break;
                    case STRING:
                        eventData = stringToREXP(eventList, expressionExecutor);
                        break;
                    case LONG:
                        eventData = longToREXP(eventList, expressionExecutor);
                        break;
                    case BOOL:
                        eventData = boolToREXP(eventList, expressionExecutor);
                        break;
                    default:
                        continue;
                }
                re.assign(inputAttributes.get(j - 2).getName(), eventData, env);
            }
            re.eval(script, env, false);
        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Unable to evaluate the script", e);
        }

        try {
            RList out = re.eval(outputs, env, true).asList();
            REXP result;
            Object[] data = new Object[out.size()];
            for (int i = 0; i < out.size(); i++) {
                result = ((REXP) out.get(i));
                switch (additionalAttributes.get(i).getType()) {
                    case BOOL:
                        if (result.isLogical()) {
                            data[i] = (result.asInteger() == 1);
                            break;
                        }
                    case INT:
                        if (result.isNumeric()) {
                            data[i] = result.asInteger();
                            break;
                        }
                    case LONG:
                        if (result.isNumeric()) {
                            data[i] = (long) result.asDouble();
                            break;
                        }
                    case FLOAT:
                        if (result.isNumeric()) {
                            data[i] = ((Double) result.asDouble()).floatValue();
                            break;
                        }
                    case DOUBLE:
                        if (result.isNumeric()) {
                            data[i] = result.asDouble();
                            break;
                        }
                    case STRING:
                        if (result.isString()) {
                            data[i] = result.asString();
                            break;
                        }
                    default:
                        throw new ExecutionPlanRuntimeException("Mismatch in returned and expected output. Expected: " +
                                additionalAttributes.get(i).getType() + " Returned: " +
                                result.asNativeJavaObject().getClass().getCanonicalName());
                }
            }
            return data;
        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Mismatch in returned output and expected output", e);
        }
    }

    protected List<Attribute> initialize(String scriptString, String outputString) {
        try {
            // Get the JRIEngine or create one
            re = JRIEngine.createEngine();
            // Create a new R environment
            env = re.newEnvironment(null, true);
        } catch (Exception e) {
            throw new ExecutionPlanCreationException("Unable to create a new session in R", e);
        }
        StreamDefinition streamDefinition;
        try {
            streamDefinition = SiddhiCompiler.parseStreamDefinition("define stream ROutputStream(" +
                    outputString + ")");

        } catch (SiddhiParserException e) {
            throw new ExecutionPlanCreationException("Could not parse the output variables string. Usage: \"a string, b int\"" +
                    ". Found: \"" + outputString + "\"", e);
        }

        List<Attribute> outputAttributes = streamDefinition.getAttributeList();

        StringBuilder sb = new StringBuilder("list(");
        for (int i = 0; i < outputAttributes.size(); i++) {
            sb.append(outputAttributes.get(i).getName());
            if (i != outputAttributes.size() - 1) {
                sb.append(",");
            }
        }
        sb.append(")");

        try {
            // Parse the output list expression
            outputs = re.parse(sb.toString(), false);
            // Parse the script
            script = re.parse(scriptString, false);
        } catch (REngineException e) {
            throw new ExecutionPlanCreationException("Unable to parse the script: " + scriptString, e);
        }
        return outputAttributes;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] objects) {

    }


    private REXP doubleToREXP(List<StreamEvent> list, ExpressionExecutor expressionExecutor) {
        double[] arr = new double[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = (Double) expressionExecutor.execute(list.get(i));
        }
        return new REXPDouble(arr);
    }

    private REXP floatToREXP(List<StreamEvent> list, ExpressionExecutor expressionExecutor) {
        double[] arr = new double[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = (Float) expressionExecutor.execute(list.get(i));
        }
        return new REXPDouble(arr);
    }

    private REXP intToREXP(List<StreamEvent> list, ExpressionExecutor expressionExecutor) {
        int[] arr = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = (Integer) expressionExecutor.execute(list.get(i));
        }
        return new REXPInteger(arr);
    }

    private REXP longToREXP(List<StreamEvent> list, ExpressionExecutor expressionExecutor) {
        double[] arr = new double[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = (Long) expressionExecutor.execute(list.get(i));
        }
        return new REXPDouble(arr);
    }

    private REXP stringToREXP(List<StreamEvent> list, ExpressionExecutor expressionExecutor) {
        String[] arr = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = (String) expressionExecutor.execute(list.get(i));
        }
        return new REXPString(arr);
    }

    private REXP boolToREXP(List<StreamEvent> list, ExpressionExecutor expressionExecutor) {
        boolean[] arr = new boolean[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = (Boolean) expressionExecutor.execute(list.get(i));
        }
        return new REXPLogical(arr);
    }
}
