/*
 * Copyright (c) 2005 - 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.wso2.siddhi.gpl.extension.nlp.dictionary;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.gpl.extension.nlp.utility.Constants;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by malithi on 8/29/14.
 */
public class Dictionary {
    private static Logger logger = Logger.getLogger(Dictionary.class);

    private HashMap<String, ArrayList<String>> store;
    private Constants.EntityType entityType;
    private String xmlFilePath;

    private static final String xsdFilePath = "dictionary.xsd";


    public Dictionary(Constants.EntityType entityType, String xmlFilePath) throws Exception {
        this.entityType = entityType;
        this.xmlFilePath = xmlFilePath;
        this.store = new HashMap<String, ArrayList<String>>();

        init();
    }

    private void init() throws Exception {
        File xmlFile = new File(xmlFilePath);
        if (!xmlFile.canRead()) {
            throw new RuntimeException("Cannot read the XML file : " + xmlFilePath);
        }

        URL xsdFileUrl = this.getClass().getClassLoader().getResource(xsdFilePath);

        Schema schema;
        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

        try {
            schema = schemaFactory.newSchema(xsdFileUrl);
        } catch (SAXException e) {
            throw new ExecutionPlanCreationException("Failed to build the schema.", e);
        }

        DictionaryHandler dictionaryHandler = new DictionaryHandler(entityType, this);

        SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
        saxParserFactory.setValidating(true);
        saxParserFactory.setSchema(schema);
        saxParserFactory.setFeature("http://apache.org/xml/features/validation/dynamic", true);

        try {
            SAXParser saxParser = saxParserFactory.newSAXParser();
            saxParser.parse(xmlFile, dictionaryHandler);

            logger.info("Dictionary XML Parse [SUCCESS]");
        } catch (SAXException e) {
            throw new ExecutionPlanCreationException("Failed to parse the given Dictionary XML file.", e);
        }
    }

    public boolean addEntry(Constants.EntityType entityType, String entry) {

        return getEntries(entityType).add(entry);
    }

    public boolean removeEntry(Constants.EntityType entityType, String entry) {

        return getEntries(entityType).remove(entry);
    }

    public synchronized ArrayList<String> getEntries(Constants.EntityType entityType) {
        ArrayList<String> entries = store.get(entityType.name());
        if (entries == null) {
            entries = new ArrayList<String>();
            store.put(entityType.name(), entries);
        }
        return entries;
    }

    public Constants.EntityType getEntityType() {
        return entityType;
    }

    public String getXmlFilePath() {
        return xmlFilePath;
    }
}
