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

package org.wso2.siddhi.gpl.extension.r;

import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.wso2.siddhi.core.config.SiddhiConfiguration;

import java.util.ArrayList;
import java.util.List;


public class RTransformTestCase {
	static final Logger log = Logger.getLogger(RTransformTestCase.class);

	protected static SiddhiConfiguration siddhiConfiguration;

	@BeforeClass
    public static void setUp() throws Exception {
		siddhiConfiguration = new SiddhiConfiguration();

		List<Class> extensions = new ArrayList<Class>(2);
		extensions.add(RSourceTransformProcessor.class);
		extensions.add(RScriptTransformProcessor.class);
		siddhiConfiguration.setSiddhiExtensions(extensions);
		log.info("RTransform tests");
	}

	
}
