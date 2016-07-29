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
package org.wso2.gpl.siddhi.extension.nlp.dictionary;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.gpl.siddhi.extension.nlp.utility.Constants;
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
