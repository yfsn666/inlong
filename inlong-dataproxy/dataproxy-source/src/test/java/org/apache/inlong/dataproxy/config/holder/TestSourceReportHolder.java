/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.dataproxy.config.holder;

import org.apache.inlong.common.heartbeat.AddressInfo;
import org.apache.inlong.dataproxy.config.ConfigManager;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Test for {@link SourceReportConfigHolder}
 */
public class TestSourceReportHolder {

    @Test
    public void testCase() {
        // first get
        Map<String, AddressInfo> srcAddressInfoMap = ConfigManager.getInstance().getSrcAddressInfos();
        Assert.assertTrue(srcAddressInfoMap.isEmpty());
        // add
        ConfigManager.getInstance().addSourceReportInfo("0.0.0.0", "46801", "INLONG", "UDP");
        ConfigManager.getInstance().addSourceReportInfo("0.0.0.0", "46801", "INLONG", "TCP");
        ConfigManager.getInstance().addSourceReportInfo("127.0.0.1", "46802", "TEST", "TCP");
        ConfigManager.getInstance().addSourceReportInfo("127.0.0.1", "46803", "TEST", "HTTP");
        ConfigManager.getInstance().addSourceReportInfo("0.0.0.0", "46801", "TEST", "TCP");
        ConfigManager.getInstance().addSourceReportInfo("127.0.0.1", "46803", "TEST", "HTTP");
        // get and check
        srcAddressInfoMap = ConfigManager.getInstance().getSrcAddressInfos();
        Assert.assertEquals(srcAddressInfoMap.size(), 4);
        String recordKey = "127.0.0.1" + "#" + "46803" + "#" + "HTTP";
        AddressInfo addressInfo = srcAddressInfoMap.get(recordKey);
        Assert.assertEquals(addressInfo.getReportSourceType(), "TEST");
    }
}
