/*
 * Copyright (C) 2016 WSO2 Inc. (http://wso2.com)
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

package org.wso2.gpl.siddhi.extension.nlp;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SentimentRate extends FunctionExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SentimentRate.class);
    private static String method;

    @Override
    public Attribute.Type getReturnType() {
        return Attribute.Type.INT;
    }

    @Override
    public void start() {
        // Nothing to do here
    }

    @Override
    public void stop() {
        // Nothing to do here
    }

    @Override
    public Object[] currentState() {
        // No need to maintain a state.
        return null;
    }

    @Override
    public void restoreState(Object[] state) {
        // No need to maintain a state
    }

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 1) {
            throw new IllegalArgumentException(
                    "Invalid no of arguments passed to SentimentRate:getSentiment() function, "
                            + "required less than 3, but found " + attributeExpressionExecutors.length);
        }
    }

    @Override
    protected Object execute(Object[] data) {
        return null;
    }

    @Override
    protected Object execute(Object data) {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        int totalRate = 0;
        String[] linesArr = data.toString().toLowerCase().split("\\.");
        for (int i = 0; i < linesArr.length; i++) {
            if (linesArr[i] != null) {
                Annotation annotation = pipeline.process(linesArr[i]);
                for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
                    Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                    int score = RNNCoreAnnotations.getPredictedClass(tree);
                    totalRate = totalRate + (score - 2);
                }
            }
        }
        return totalRate;
    }
}