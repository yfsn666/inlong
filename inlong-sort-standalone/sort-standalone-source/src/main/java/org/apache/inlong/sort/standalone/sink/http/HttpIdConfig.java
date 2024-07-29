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

package org.apache.inlong.sort.standalone.sink.http;

import org.apache.inlong.common.pojo.sort.dataflow.DataFlowConfig;
import org.apache.inlong.common.pojo.sort.dataflow.field.FieldConfig;
import org.apache.inlong.common.pojo.sort.dataflow.sink.HttpSinkConfig;
import org.apache.inlong.sort.standalone.config.pojo.IdConfig;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@EqualsAndHashCode(callSuper = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class HttpIdConfig extends IdConfig {

    private String path;
    private String method = "POST";
    private Map<String, String> headers;
    private String dataFormatClass;
    private String separator = "|";
    private List<String> fieldList;

    public static HttpIdConfig create(DataFlowConfig dataFlowConfig) {
        HttpSinkConfig sinkConfig = (HttpSinkConfig) dataFlowConfig.getSinkConfig();
        List<String> fields = sinkConfig.getFieldConfigs()
                .stream()
                .map(FieldConfig::getName)
                .collect(Collectors.toList());
        return HttpIdConfig.builder()
                .inlongGroupId(dataFlowConfig.getInlongGroupId())
                .inlongStreamId(dataFlowConfig.getInlongStreamId())
                .path(sinkConfig.getPath())
                .method(sinkConfig.getMethod())
                .headers(sinkConfig.getHeaders())
                .dataFormatClass(sinkConfig.getDataFormatClass())
                .separator(sinkConfig.getSeparator())
                .fieldList(fields)
                .build();
    }
}
