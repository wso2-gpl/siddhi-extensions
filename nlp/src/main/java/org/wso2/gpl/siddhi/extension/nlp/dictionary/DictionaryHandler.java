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
import org.wso2.gpl.siddhi.extension.nlp.utility.Constants;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * Created by malithi on 8/29/14.
 */
public class DictionaryHandler extends DefaultHandler {
    private static Logger logger = Logger.getLogger(DictionaryHandler.class);

    private Constants.EntityType entityType;
    private Dictionary dictionary;
    private String value = null;
    private boolean read = false;

    public DictionaryHandler(Constants.EntityType entityType, Dictionary dictionary) {
        this.entityType = entityType;
        this.dictionary = dictionary;
    }

    @Override
    public void startDocument() throws SAXException {
        logger.info("Loading dictionary ...");
    }

    @Override
    public void endDocument() throws SAXException {
        logger.info("Dictionary loading [COMPLETED]");
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if (qName.equalsIgnoreCase(Constants.DictionaryTag.ENTITY.getTag())) {
            read = attributes.getValue(Constants.DictionaryTag.ID.getTag()).equalsIgnoreCase(entityType.name());
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if (read) {
            if (qName.equalsIgnoreCase(Constants.DictionaryTag.ENTRY.getTag())) {
                dictionary.addEntry(entityType, value);
            }
        }
    }

    @Override
    public void characters(char[] chars, int start, int length) throws SAXException {
        value = String.copyValueOf(chars, start, length).trim();
    }
}
