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

package org.wso2.gpl.siddhi.extension.evalscript;


import org.rosuda.REngine.JRI.JRIEngine;
import org.rosuda.REngine.*;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.function.EvalScript;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.Attribute.Type;

public class EvalR implements EvalScript {

    private REngine rEngine;
    private REXP env;
    private REXP functionCall;
    private String functionName;
    private Attribute.Type returnType;

    @Override
    public void init(String name, String body) {
        this.functionName = name;
        try {
            // Get the JRIEngine or create one
            rEngine = JRIEngine.createEngine();
            // Create a new R environment
            env = rEngine.newEnvironment(null, true);

        } catch (Exception e) {
            throw new ExecutionPlanCreationException("Error while initializing the REngine", e);
        }

        try {
            // Define the function in R environment env
            rEngine.parseAndEval(name + " <- function(data) { " + body + " }",
                    env, false);
            // Parse the function call in R
            functionCall = rEngine.parse(name + "(data)", false);
        } catch (Exception e) {
            throw new ExecutionPlanCreationException("Compilation failure of the R function " + name, e);
        }
    }

    @Override
    public Object eval(String name, Object[] arg) {
        REXP[] data = new REXP[arg.length];
        for (int i = 0; i < arg.length; i++) {
            data[i] = REXPWrapper.wrap(arg[i]);
        }

        try {
            // Send the data to R and assign it to symbol 'data'
            rEngine.assign("data", new REXPExpressionVector(new RList(data)), env);
            // Execute the function call
            REXP result = rEngine.eval(functionCall, env, true);
            switch (returnType) {
                case BOOL:
                    if (result.isLogical()) {
                        return result.asInteger() == 1;
                    }
                    break;
                case INT:
                    if (result.isInteger()) {
                        return result.asInteger();
                    }
                    break;
                case LONG:
                    if (result.isNumeric()) {
                        return ((long) result.asDouble());
                    }
                    break;
                case FLOAT:
                    if (result.isNumeric()) {
                        return ((Double) result.asDouble()).floatValue();
                    }
                    break;
                case DOUBLE:
                    if (result.isNumeric()) {
                        return ((Double) result.asDouble());
                    }
                    break;
                case STRING:
                    if (result.isString()) {
                        return result.asString();
                    }
                    break;
                default:
                    break;
            }
            throw new ExecutionPlanRuntimeException(
                    "Wrong return type detected. Expected: " + returnType
                            + " found: " + result.asNativeJavaObject().getClass().getCanonicalName());

        } catch (Exception e) {
            throw new ExecutionPlanRuntimeException("Error evaluating R function " + functionName, e);
        }
    }

    @Override
    public void setReturnType(Type returnType) {
        this.returnType = returnType;
    }

    @Override
    public Type getReturnType() {
        return returnType;
    }
}