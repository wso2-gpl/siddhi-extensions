package org.wso2.cep.geo.libs;

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

// TODO: What about moving this logic to governance registry local ?
public class ExecutionPlansCount {
    public static Integer numberOfExecutionPlans = 0;

    public static Integer getNumberOfExecutionPlans(){
        return (Integer)ExecutionPlansCount.numberOfExecutionPlans/2;
    }

    public static void setNumberOfExecutionPlans(Integer numberOfExecutionPlans) {
        ExecutionPlansCount.numberOfExecutionPlans = numberOfExecutionPlans;
    }

    public static void upCount(){
        ExecutionPlansCount.numberOfExecutionPlans +=1;
    }

    public static void downCount(){
        // TODO: -1 is due to a bug(https://wso2.org/jira/browse/CEP-953) in current version of CEP, when it is fixed need to change the -2 to -1 here and also in getNumberOfExecutionPlans '/2'
        ExecutionPlansCount.numberOfExecutionPlans -=1;
    }
}
