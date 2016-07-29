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
