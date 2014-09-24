package org.wso2.siddhi.gpl.extension.nlp.utility;

import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.expression.constant.*;
import sun.misc.DoubleConsts;

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
