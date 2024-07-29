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

package org.apache.inlong.common.pojo.sort.node;

import lombok.Data;

@Data
public class HttpNodeConfig extends NodeConfig {

    private String domain;
    private Integer port;
    private Integer maxConnect;
    private Integer keywordMaxLength;
    private Integer maxThreads;
    private String auditSetName;
    private String httpHosts;
    private Boolean enableCredential;
    private String username;
    private String password;
    private Boolean enableToken;
    private String token;
}
