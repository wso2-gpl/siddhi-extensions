/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *   WSO2 Inc. licenses this file to you under the Apache License,
 *   Version 2.0 (the "License"); you may not use this file except
 *   in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.siddhi.gpl.extension.nlp;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.config.SiddhiConfiguration;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by malithi on 9/10/14.
 */
public abstract class NlpTransformProcessorTest {

    private static Logger logger = Logger.getLogger(NlpTransformProcessorTest.class);

    protected static SiddhiManager siddhiManager;
    protected static List<String[]> data;

    protected long start;
    protected long end;

    public abstract void setUpChild();

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("Reading data");

        data = new ArrayList<String[]>();

        InputStream inputStream = ClassLoader.getSystemResourceAsStream("data.csv");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = bufferedReader.readLine()) != null){
            String[] dataLine = line.split(",",2);
            data.add(dataLine);
        }

        inputStream.close();
        bufferedReader.close();

        logger.info("Init Siddhi");

        SiddhiConfiguration siddhiConfiguration = new SiddhiConfiguration();

        List<Class> extensions = new ArrayList<Class>(6);
        extensions.add(NameEntityTypeTransformProcessor.class);
        extensions.add(NameEntityTypeViaDictionaryTransformProcessor.class);
        extensions.add(RelationshipByRegexTransformProcessor.class);
        extensions.add(RelationshipByVerbTransformProcessor.class);
        extensions.add(SemgrexPatternTransformProcessor.class);
        extensions.add(TokensRegexPatternTransformProcessor.class);

        siddhiConfiguration.setSiddhiExtensions(extensions);

        siddhiManager = new SiddhiManager(siddhiConfiguration);

    }

    @Before
    public void setUpChildren(){
        setUpChild();
    }

    @AfterClass
    public static void tearDown() throws Exception{
        Thread.sleep(1000);
        logger.info("Shutting down Siddhi");
        siddhiManager.shutdown();
    }

}
