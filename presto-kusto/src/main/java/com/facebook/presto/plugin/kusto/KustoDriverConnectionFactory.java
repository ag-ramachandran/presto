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
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class KustoDriverConnectionFactory
        implements ConnectionFactory {
    private final String hostName;
    private final Properties connectionProperties;
    private final Optional<String> appId;
    private final Optional<String> appKey;
    private final Optional<String> tenantId;

    public KustoDriverConnectionFactory(KustoConnectionConfig config) {
        this(
                config.getHostName(),
                Optional.ofNullable(config.getAppId()),
                Optional.ofNullable(config.getAppKey()),
                Optional.ofNullable(config.getTenantId()),
                basicConnectionProperties(config));
    }

    public KustoDriverConnectionFactory(String hostName, Optional<String> appId, Optional<String> appKey, Optional<String> tenantId, Properties connectionProperties) {
        this.hostName = requireNonNull(hostName, "connectionUrl is null");
        this.connectionProperties = new Properties();
        this.connectionProperties.putAll(requireNonNull(connectionProperties, "connectionProperties is null"));
        this.appId = requireNonNull(appId, "appId is null");
        this.appKey = requireNonNull(appKey, "appKey is null");
        this.tenantId = requireNonNull(tenantId, "tenantId is null");
    }

    public static Properties basicConnectionProperties(KustoConnectionConfig config) {
        Properties connectionProperties = new Properties();
        if (config.getAppId() != null) {
            connectionProperties.setProperty("app-id", config.getAppId());
        }
        if (config.getAppKey() != null) {
            connectionProperties.setProperty("app-key", config.getAppKey());
        }
        return connectionProperties;
    }

    private static void setConnectionProperty(Properties connectionProperties, Map<String, String> extraCredentials, String credentialName, String propertyName) {
        String value = extraCredentials.get(credentialName);
        if (value != null) {
            connectionProperties.setProperty(propertyName, value);
        }
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException {
        Properties updatedConnectionProperties;
        if (appId.isPresent() || appKey.isPresent()) {
            updatedConnectionProperties = new Properties();
            updatedConnectionProperties.putAll(connectionProperties);
            appId.ifPresent(credentialName -> setConnectionProperty(updatedConnectionProperties, identity.getExtraCredentials(), credentialName, "appId"));
            appKey.ifPresent(credentialName -> setConnectionProperty(updatedConnectionProperties, identity.getExtraCredentials(), credentialName, "appKey"));
            tenantId.ifPresent(credentialName -> setConnectionProperty(updatedConnectionProperties, identity.getExtraCredentials(), credentialName, "tenantId"));
        } else {
            updatedConnectionProperties = connectionProperties;
        }

        Connection connection = null;
        try {
            ConfidentialClientApplication app = ConfidentialClientApplication.builder(
                            updatedConnectionProperties.getProperty("appId"),
                            ClientCredentialFactory.createFromSecret(updatedConnectionProperties.getProperty("appKey")))
                    .authority(String.format("https://login.microsoftonline.com/%s", updatedConnectionProperties.getProperty("tenantId")))
                    .build();
            ClientCredentialParameters clientCredentialParam = ClientCredentialParameters.builder(
                            Collections.singleton("scope"))
                    .build();
            CompletableFuture<IAuthenticationResult> future = app.acquireToken(clientCredentialParam);
            IAuthenticationResult authResult = future.get();

            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName("<your cluster DNS name>");
            ds.setDatabaseName("<your database name>");
            ds.setAccessToken(authResult.accessToken());
            ds.setHostNameInCertificate("*.kusto.windows.net");
            connection = ds.getConnection();
            checkState(connection != null, "Driver returned null connection");
            return connection;

        } catch (MalformedURLException | ExecutionException | InterruptedException e) {
            checkState(connection != null, "Driver returned null connection");
            throw new RuntimeException(e);
        }
    }
}