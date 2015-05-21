/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.siddhi.gpl.extension.pmml;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.xml.sax.InputSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.PMMLManager;
import org.jpmml.model.ImportFilter;
import org.jpmml.model.JAXBUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.transform.Source;

public class PmmlModelProcessor extends StreamProcessor {

    private static final Log logger = LogFactory.getLog(PmmlModelProcessor.class);
    private static final String PREDICTION = "prediction";

    private String pmmlDefinition;
    private boolean attributeSelectionAvailable;
    
    //TODO: This should be a map of <feature_name, attribute_index>
    private Map<Integer, Integer> attributeIndexMap;           // <feature-index, attribute-index> pairs
    
    private List<FieldName> inputFields;        // All the input fields defined in the pmml definition
    private List<FieldName> outputFields;       // Output fields of the pmml definition

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        StreamEvent event = streamEventChunk.getFirst();

        /*Object[] data;
        double[] featureValues;
        if (attributeSelectionAvailable) {
            data = event.getBeforeWindowData();
            featureValues = new double[data.length];
        } else {
            data = event.getOutputData();
            featureValues = new double[data.length-1];
        }

        for(Map.Entry<Integer, Integer> entry : attributeIndexMap.entrySet()) {
            int featureIndex = entry.getKey();
            int attributeIndex = entry.getValue();
            featureValues[featureIndex] = Double.parseDouble(String.valueOf(data[attributeIndex]));
        }

        if(featureValues != null) {
            try {
                double predictionResult = modelHandler.predict(featureValues);
                Object[] output = new Object[]{predictionResult};
                complexEventPopulater.populateComplexEvent(event, output);
                nextProcessor.process(streamEventChunk);
            } catch (Exception e) {
                log.error("Error while predicting", e);
                throw new ExecutionPlanRuntimeException("Error while predicting" ,e);
            }
        }*/
    }

    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {

        if(attributeExpressionExecutors.length == 0) {
            throw new ExecutionPlanValidationException("PMML model definition not available.");
        } else if(attributeExpressionExecutors.length == 1) {
            attributeSelectionAvailable = false;    // model-definition only
        } else {
            attributeSelectionAvailable = true;  // model-definition and stream-attributes list
        }

        // Check whether the first parameter in the expression is the pmml definition
        if(attributeExpressionExecutors[0] instanceof ConstantExpressionExecutor)  {
            Object constantObj = ((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue();
            pmmlDefinition = (String) constantObj;
        } else {
            throw new ExecutionPlanValidationException("PMML model definition has not been set as the first parameter");
        }
        
        // Unmarshal the definition and get an executable pmml model
        PMML pmmlModel = unmarshal(pmmlDefinition);
        // Get the different types of fields defined in the pmml model
        PMMLManager pmmlManager = new PMMLManager(pmmlModel);
        
        Evaluator evaluator =(Evaluator) pmmlManager.getModelManager(ModelEvaluatorFactory.getInstance());
        inputFields = evaluator.getActiveFields();
        outputFields = evaluator.getOutputFields();
        
        return Arrays.asList(new Attribute(PREDICTION, Attribute.Type.DOUBLE));
    }

    @Override
    public void start() {
       /* try {
            modelHandler = new ModelHandler(pmmlDefinition);
            populateFeatureAttributeMapping();
        } catch (Exception e) {
            log.error("Error while retrieving ML-model : " + pmmlDefinition, e);
            throw new ExecutionPlanCreationException("Error while retrieving ML-model : " + pmmlDefinition + "\n" + e.getMessage());
        }*/
    }

    /**
     * Match the attribute index values of stream with feature index value of the model
     * @throws Exception
     */
    private void populateFeatureAttributeMapping() throws Exception {
        
        // TODO: Compare the siddhi parameters against the inputFields of pmml definition.
        
        attributeIndexMap = new HashMap<Integer, Integer>();
        Map<String, Integer> featureIndexMap = modelHandler.getFeatures();

        if(attributeSelectionAvailable) {
            int index = 0;
            for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
                if(expressionExecutor instanceof VariableExpressionExecutor) {
                    VariableExpressionExecutor variable = (VariableExpressionExecutor) expressionExecutor;
                    String variableName = variable.getAttribute().getName();
                    if (featureIndexMap.get(variableName) != null) {
                        int featureIndex = featureIndexMap.get(variableName);
                        int attributeIndex = index;
                        attributeIndexMap.put(featureIndex, attributeIndex);
                    } else {
                        throw new ExecutionPlanCreationException("No matching feature name found in the model " +
                                "for the attribute : " + variableName);
                    }
                    index++;
                }
            }
        } else {
            String[] attributeNames = inputDefinition.getAttributeNameArray();
            for(String attributeName : attributeNames) {
                if (featureIndexMap.get(attributeName) != null) {
                    int featureIndex = featureIndexMap.get(attributeName);
                    int attributeIndex = inputDefinition.getAttributePosition(attributeName);
                    attributeIndexMap.put(featureIndex, attributeIndex);
                } else {
                    throw new ExecutionPlanCreationException("No matching feature name found in the model " +
                            "for the attribute : " + attributeName);
                }
            }
        }
    }

    /**
     * TODO : move to a Util class (PmmlUtil)
     * Unmarshal the definition and get an executable pmml model.
     * 
     * @return  pmml model
     */
    private PMML unmarshal(String pmmlDefinition) {
        try {
            File pmmlFile = new File(pmmlDefinition);
            InputSource pmmlSource;
            Source source;
            // if the given is a file path, read the pmml definition from the file
            if (pmmlFile.isFile() && pmmlFile.canRead()) {
                pmmlSource = new InputSource(new FileInputStream(pmmlFile));
            } else {
                // else, read from the given definition
                pmmlSource = new InputSource(new StringReader(pmmlDefinition));
            }
            source = ImportFilter.apply(pmmlSource);
            return JAXBUtil.unmarshalPMML(source);
        } catch (Exception e) {
            logger.error("Failed to unmarshal the pmml definition: " + e.getMessage());
            throw new ExecutionPlanCreationException("Failed to unmarshal the pmml definition: " + pmmlDefinition + ". " 
                    + e.getMessage(), e);
        }
    }
    
    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[0];
    }

    @Override
    public void restoreState(Object[] state) {

    }
}
