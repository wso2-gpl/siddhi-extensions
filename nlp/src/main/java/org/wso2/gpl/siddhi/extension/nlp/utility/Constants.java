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

package org.wso2.gpl.siddhi.extension.nlp.utility;

import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.*;

/**
 * Created by malithi on 9/5/14.
 */
public class Constants {
    private Constants() {
    }

    public enum EntityType{
        PERSON, LOCATION, ORGANIZATION, MONEY, PERCENT, DATE, TIME
    }

    public enum DictionaryTag{
        ENTITY("entity"),
        ENTRY("entry"),
        ID("id");

        private String tag;

        DictionaryTag(String tag) {
            this.tag = tag;
        }

        public String getTag() {
            return tag;
        }
    }

    public static final String subject = "subject";
    public static final String object = "object";
    public static final String verb = "verb";

    private static final String typeBoolean = "boolean";
    private static final String typeString = "string";
    private static final String typeInt = "int";
    private static final String typeDouble = "double";
    private static final String typeFloat = "float";
    private static final String typeLong = "long";
    private static final String typeVariable = "variable";
    private static final String typeUnknown = "unknown";

    public static String getType(Expression expression){
        if (expression instanceof BoolConstant){
            return typeBoolean;
        }else if(expression instanceof StringConstant){
            return typeString;
        }else if(expression instanceof IntConstant){
            return typeInt;
        }else if(expression instanceof DoubleConstant){
            return typeDouble;
        }else if(expression instanceof FloatConstant){
            return typeFloat;
        }else if(expression instanceof LongConstant){
            return typeLong;
        }else if(expression instanceof Variable){
            return typeVariable;
        }

        return typeUnknown;
    }
}
