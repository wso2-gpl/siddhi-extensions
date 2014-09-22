package org.wso2.siddhi.gpl.extension.nlp.utility;

/**
 * Created by malithi on 9/5/14.
 */
public class Constants {
    private Constants() {
    }

    public enum EntityType{
        PERSON, LOCATION, ORGANIZATION, MONEY, PERCENT, DATE, TIME;
    }

    public enum DictionaryTag{
        DICTIONARY("dictionary"),
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
}
