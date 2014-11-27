package org.wso2.cep.geo.notify;

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
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.extension.annotation.SiddhiExtension;

import java.util.HashMap;

@SiddhiExtension(namespace = "geo", function = "needToNotify")
public class NotifyAlert extends FunctionExecutor {

    //    Logger log = Logger.getLogger(CustomFunctionExtension.class);
    Attribute.Type returnType;
    String previousInformation;
    HashMap<String, String> informationBuffer = new HashMap<String, String>();

    /**
     * Method will be called when initialising the custom function
     *
     * @param types
     * @param siddhiContext
     */
    @Override
    public void init(Attribute.Type[] types, SiddhiContext siddhiContext) {
        for (Attribute.Type attributeType : types) {
            if (!(attributeType == Attribute.Type.STRING)) {
                throw new QueryCreationException("Information should be a string value");
            }
        }
        returnType = Attribute.Type.BOOL;
    }

    /**
     * Method called when sending events to process
     *
     * @param obj
     * @return
     */
    @Override
    protected Object process(Object obj) {
        Boolean returnValue = false;
        Object[] objects = (Object[]) obj;
        String id = (String) objects[0];
        String currentInformation = (String) objects[1];
        if (informationBuffer.containsKey(id) && !informationBuffer.get(id).equals(currentInformation)) {
            returnValue = true;
        }
        informationBuffer.put(id, currentInformation);
        return returnValue;
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
