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

import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class KustoDriverConnectionFactory
        implements ConnectionFactory
{
    private final String hostName;
    private final Properties connectionProperties;
    private final String appId;
    private final String appKey;
    private final String tenantId;
    private final String database;

    public KustoDriverConnectionFactory(KustoConnectionConfig config)
    {
        this(
                config.getHostName(),
                config.getAppId(),
                config.getAppKey(),
                config.getTenantId(),
                config.getDatabase(),
                basicConnectionProperties(config));
    }

    public KustoDriverConnectionFactory(String hostName, String appId, String appKey, String tenantId,
                                        String database, Properties connectionProperties)
    {
        this.hostName = requireNonNull(hostName, "hostName is null");
        this.appId = requireNonNull(appId, "appId is null");
        this.appKey = requireNonNull(appKey, "appKey is null");
        this.tenantId = requireNonNull(tenantId, "tenantId is null");
        this.database = requireNonNull(database, "database is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "connectionProperties is null"));
    }

    public static Properties basicConnectionProperties(KustoConnectionConfig config)
    {
        Properties connectionProperties = new Properties();
        if (config.getHostName() != null) {
            connectionProperties.setProperty("hostName", config.getHostName());
        }
        if (config.getAppId() != null) {
            connectionProperties.setProperty("appKey", config.getAppId());
        }
        if (config.getAppKey() != null) {
            connectionProperties.setProperty("appKey", config.getAppKey());
        }
        if (config.getTenantId() != null) {
            connectionProperties.setProperty("tenantId", config.getTenantId());
        }
        if (config.getDatabase() != null) {
            connectionProperties.setProperty("database", config.getDatabase());
        }
        return connectionProperties;
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        Connection connection = null;
        try {
            ConfidentialClientApplication app = ConfidentialClientApplication.builder(
                            connectionProperties.getProperty("appId"),
                            ClientCredentialFactory.createFromSecret(connectionProperties.getProperty("appKey")))
                    .authority(String.format("https://login.microsoftonline.com/%s", connectionProperties.getProperty("tenantId")))
                    .build();
            ClientCredentialParameters clientCredentialParam = ClientCredentialParameters.builder(
                            Collections.singleton(String.format("https://%s/.default", connectionProperties.getProperty("hostName"))))
                    .build();
            CompletableFuture<IAuthenticationResult> future = app.acquireToken(clientCredentialParam);
            IAuthenticationResult authResult = future.get();
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(connectionProperties.getProperty("hostName"));
            ds.setDatabaseName(connectionProperties.getProperty("database"));
            ds.setAccessToken(authResult.accessToken());
            ds.setHostNameInCertificate("*.z9trident.dev.kusto.windows.net");
            connection = ds.getConnection();
            checkState(connection != null, "Driver returned null connection");
            return connection;
        }
        catch (MalformedURLException | ExecutionException | InterruptedException e) {
            checkState(connection != null, "Driver returned null connection");
            throw new RuntimeException(e);
        }
    }
}
