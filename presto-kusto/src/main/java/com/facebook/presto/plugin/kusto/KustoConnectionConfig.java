/*
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
package com.facebook.presto.plugin.kusto;

import com.facebook.airlift.configuration.Config;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;

import javax.validation.constraints.NotNull;

public class KustoConnectionConfig extends BaseJdbcConfig
{
    private String appId;
    private String appKey;
    private String tenantId;
    private String hostName;
    private String database;
    @NotNull
    public String getAppId()
    {
        return appId;
    }

    @Config("kusto.appId")
    public KustoConnectionConfig setAppId(String appId)
    {
        this.appId = appId;
        return this;
    }
    @NotNull
    public String getAppKey()
    {
        return appKey;
    }

    @Config("kusto.appKey")
    public KustoConnectionConfig setAppKey(String appKey)
    {
        this.appKey = appKey;
        return this;
    }
    @NotNull
    public String getTenantId()
    {
        return tenantId;
    }
    @Config("kusto.tenantId")
    public KustoConnectionConfig setTenantId(String tenantId)
    {
        this.tenantId = tenantId;
        return this;
    }
    public @NotNull String getHostName()
    {
        return this.hostName;
    }
    @Config("kusto.hostName")
    public KustoConnectionConfig setHostName(String hostName)
    {
        this.hostName = hostName;
        return this;
    }

    public @NotNull String getDatabase()
    {
        return this.database;
    }
    @Config("kusto.database")
    public KustoConnectionConfig setDatabase(String database)
    {
        this.database = database;
        return this;
    }
}
