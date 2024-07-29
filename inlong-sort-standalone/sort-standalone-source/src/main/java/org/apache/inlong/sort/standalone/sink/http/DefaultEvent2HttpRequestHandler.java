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

import org.apache.inlong.sort.standalone.channel.ProfileEvent;
import org.apache.inlong.sort.standalone.utils.UnescapeHelper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * DefaultEvent2HttpRequestHandler
 */
public class DefaultEvent2HttpRequestHandler implements IEvent2HttpRequestHandler {

    /**
     * parse
     *
     * @param context
     * @param event
     * @return
     */
    @Override
    public HttpRequest parse(HttpSinkContext context, ProfileEvent event)
            throws URISyntaxException, JsonProcessingException {
        String uid = event.getUid();
        HttpIdConfig idConfig = context.getIdConfig(uid);
        if (idConfig == null) {
            context.addSendResultMetric(event, context.getTaskName(), false, System.currentTimeMillis());
            return null;
        }
        // get the dataformat class
        String delimeter = idConfig.getSeparator();
        char cDelimeter = delimeter.charAt(0);
        // for tab separator
        byte[] bodyBytes = event.getBody();
        String strContext = new String(bodyBytes, Charset.defaultCharset());
        // unescape
        List<String> columnVlues = UnescapeHelper.toFiledList(strContext, cDelimeter);
        int valueLength = columnVlues.size();
        List<String> fieldList = idConfig.getFieldList();
        int columnLength = fieldList.size();
        // get field value
        Map<String, String> fieldMap = new HashMap<>();
        for (int i = 0; i < columnLength; ++i) {
            String fieldName = fieldList.get(i);
            String fieldValue = i < valueLength ? columnVlues.get(i) : "";
            byte[] fieldBytes = fieldValue.getBytes(Charset.defaultCharset());
            if (fieldBytes.length > context.getKeywordMaxLength()) {
                fieldValue = new String(fieldBytes, 0, context.getKeywordMaxLength());
            }
            fieldMap.put(fieldName, fieldValue);
        }

        // build
        String uriString;
        if (context.getPort() == 0) {
            uriString = context.getDomain() + idConfig.getPath();
        } else {
            uriString = context.getDomain() + ":" + context.getPort() + idConfig.getPath();
        }
        URI uri = new URI(uriString);
        String jsonData = new ObjectMapper().writeValueAsString(fieldMap);
        HttpUriRequest request;
        switch (idConfig.getMethod().toUpperCase()) {
            case "GET":
                String params = fieldMap.entrySet().stream()
                        .map(entry -> {
                            try {
                                return URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8.name()) + "="
                                        + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8.name());
                            } catch (UnsupportedEncodingException e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .collect(Collectors.joining("&"));
                request = new HttpGet(uri + "?" + params);
                for (Map.Entry<String, String> entry : idConfig.getHeaders().entrySet()) {
                    request.setHeader(entry.getKey(), entry.getValue());
                }
                if (context.getEnableToken()) {
                    request.setHeader("Authorization", "Bearer " + context.getToken());
                }
                break;
            case "POST":
                request = new HttpPost(uri);
                for (Map.Entry<String, String> entry : idConfig.getHeaders().entrySet()) {
                    request.setHeader(entry.getKey(), entry.getValue());
                }
                if (context.getEnableToken()) {
                    request.setHeader("Authorization", "Bearer " + context.getToken());
                }
                setEntity((HttpEntityEnclosingRequestBase) request, jsonData);
                break;
            case "PUT":
                request = new HttpPut(uri);
                for (Map.Entry<String, String> entry : idConfig.getHeaders().entrySet()) {
                    request.setHeader(entry.getKey(), entry.getValue());
                }
                if (context.getEnableToken()) {
                    request.setHeader("Authorization", "Bearer " + context.getToken());
                }
                setEntity((HttpEntityEnclosingRequestBase) request, jsonData);
                break;
            default:
                throw new RuntimeException("Unsupported method: " + idConfig.getMethod());
        }
        return new HttpRequest(request, event);
    }

    private static void setEntity(HttpEntityEnclosingRequestBase request, String jsonData) {
        StringEntity requestEntity = new StringEntity(jsonData, ContentType.APPLICATION_JSON);
        request.setEntity(requestEntity);
    }
}
