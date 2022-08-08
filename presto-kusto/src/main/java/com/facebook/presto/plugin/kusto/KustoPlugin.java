package com.facebook.presto.plugin.kusto;


import com.facebook.presto.plugin.jdbc.JdbcPlugin;

public class KustoPlugin
        extends JdbcPlugin {
    public KustoPlugin() {
        super("kusto", new KustoClientModule());
    }
}