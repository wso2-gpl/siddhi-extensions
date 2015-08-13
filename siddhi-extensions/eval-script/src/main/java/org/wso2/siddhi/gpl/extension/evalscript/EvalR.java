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

package org.wso2.siddhi.gpl.extension.evalscript;


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