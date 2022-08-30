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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KustoDriverConnectionFactory
        extends DriverConnectionFactory
{
    private static final Logger log = Logger.get(KustoDriverConnectionFactory.class);

    private static final String regex = ";databaseName|database=([^;]*)";

    private final String schemaName;

    public KustoDriverConnectionFactory(Driver driver, BaseJdbcConfig config)
    {
        super(driver, config);
        schemaName = getDatabaseName(config.getConnectionUrl());
    }

    @Override
    public Connection openConnection(JdbcIdentity identity)
            throws SQLException
    {
        Connection connection = super.openConnection(identity);
        connection.setSchema(schemaName);
        return connection;
    }

    private String getDatabaseName(String connectionUrl)
    {
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(connectionUrl);
        if (matcher.find()) {
            return matcher.group();
        }
        log.warn("Could not extract database/databaseName from connectionUrl");
        return "dbo";
    }
}
