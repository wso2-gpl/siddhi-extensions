package org.wso2.cep.geo.functions;

/*
*  Copyright (c) 2005-2010, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
*  WSO2 Inc. licenses this file to you under the Apache License,
*  Version 2.0 (the "License"); you may not use this file except
*  in compliance with the License.
*  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.UUID;

@SiddhiExtension(namespace = "geo", function = "generateEventId")
public class EventIdGenerator extends FunctionExecutor {

//    Logger log = Logger.getLogger(CustomFunctionExtension.class);
    Attribute.Type returnType;

    /**
     * Method will be called when initialising the custom function
     *
     * @param types
     * @param siddhiContext
     */
    @Override
    public void init(Attribute.Type[] types, SiddhiContext siddhiContext) {

//        -For reference-
/*        for (Attribute.Type attributeType : types) {
            if (attributeType == Attribute.Type.DOUBLE) {
                returnType = attributeType;
                break;
            } else if ((attributeType == Attribute.Type.STRING) || (attributeType == Attribute.Type.BOOL)) {
                throw new QueryCreationException("Plus cannot have parameters with types String or Bool");
            } else {
                returnType = Attribute.Type.LONG;
            }

        }*/
        returnType = Attribute.Type.STRING;
    }

    /**
     * Method called when sending events to process
     *
     * @param obj
     * @return
     */
    @Override
    protected Object process(Object obj) {
// -for reference-
/*        if (returnType == Attribute.Type.DOUBLE) {
            double total = 0;
            if (obj instanceof Object[]) {
                for (Object aObj : (Object[]) obj) {
                    total += Double.parseDouble(String.valueOf(aObj));
                }
            }
            return total;
        } else {
            long total = 0;
            if (obj instanceof Object[]) {
                for (Object aObj : (Object[]) obj) {
                    total += Long.parseLong(String.valueOf(aObj));
                }
            }
            return total;
        }*/

        return UUID.randomUUID().toString();
    }

    @Override
    public void destroy() {

    }

    /**
     * Return type of the custom function mentioned
     *
     * @return
     */
    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }


}
