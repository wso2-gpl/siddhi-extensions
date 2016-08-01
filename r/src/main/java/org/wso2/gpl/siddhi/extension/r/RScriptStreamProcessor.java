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

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.List;

public class RScriptStreamProcessor extends RStreamProcessor {
    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

        if (attributeExpressionExecutors.length < 2) {
            throw new ExecutionPlanCreationException("Wrong number of attributes given. Expected 2 or more, found " +
                    attributeExpressionLength+ "\n" +
                    "Usage: #R:eval(script:string, outputVariables:string, input1, ...)");
        }
        String scriptString;
        String outputString;

        try {
            if (!(attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor)) {
                throw new ExecutionPlanCreationException("First parameter should be a constant");
            }
            scriptString = (String) attributeExpressionExecutors[0].execute(null);
        } catch (ClassCastException e) {
            throw new ExecutionPlanCreationException("First parameter should be of type string. Found " +
                    attributeExpressionExecutors[0].execute(null).getClass().getCanonicalName() + "\n" +
                    "Usage: #R:eval(script:string, outputVariables:string, input1, ...)");
        }
        try {
            if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                throw new ExecutionPlanCreationException("Second parameter should be a constant");
            }
            outputString = (String) attributeExpressionExecutors[1].execute(null);
        } catch (ClassCastException e) {
            throw new ExecutionPlanCreationException("Second parameter should be of type string. Found " +
                    attributeExpressionExecutors[1].execute(null).getClass().getCanonicalName() + "\n" +
                    "Usage: #R:eval(script:string, outputVariables:string, input1, ...)");
        }

        for (int i = 2; i < attributeExpressionLength; i++) {
            if (attributeExpressionExecutors[i] instanceof VariableExpressionExecutor) {
                inputAttributes.add(((VariableExpressionExecutor) attributeExpressionExecutors[i]).getAttribute());
            } else {
                throw new ExecutionPlanCreationException("Parameter " + (i + 1) + " should be a variable");
            }
        }
        return initialize(scriptString, outputString);
    }
}
