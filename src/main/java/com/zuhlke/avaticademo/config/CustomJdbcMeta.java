package com.zuhlke.avaticademo.config;

import com.google.common.cache.Cache;
import org.apache.calcite.avatica.jdbc.JdbcMeta;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

public class CustomJdbcMeta extends JdbcMeta {

    private final DataSource dataSource;

    public CustomJdbcMeta(String url, Properties properties, DataSource dataSource) throws SQLException {
        super(url, properties);
        this.dataSource = dataSource;
    }

    @Override
    public void openConnection(ConnectionHandle ch, Map<String, String> info) {

        Properties fullInfo = new Properties();
        try {
            final Field fieldInfo = getField("info");
            fullInfo.putAll((Map<?, ?>) fieldInfo.get(this));

            if (info != null) {
                fullInfo.putAll(info);
            }

            Field fieldConnectionCache = getField("connectionCache");
            Cache<String, Connection> connectionCache = (Cache<String, Connection>) fieldConnectionCache.get(this);
            ConcurrentMap<String, Connection> cacheAsMap = connectionCache.asMap();

            if (cacheAsMap.containsKey(ch.id)) {
                throw new RuntimeException("Connection already exists: " + ch.id);
            } else {
                try {
                    Field fieldUrl = getField("url");
                    Connection conn = this.createConnection((String) fieldUrl.get(this), fullInfo);
                    Connection loadedConn = cacheAsMap.putIfAbsent(ch.id, conn);
                    if (loadedConn != null) {
                        conn.close();
                        throw new RuntimeException("Connection already exists: " + ch.id);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Connection createConnection(String url, Properties info) {

        Connection connection;
        try {
            connection = dataSource.getConnection();

        } catch (SQLException ex) {
            connection = getSharedConnection();
            return connection;
        }
        return connection;
    }

    private Connection getSharedConnection() {
        Field fieldConnectionCache;
        try {

            fieldConnectionCache = getField("connectionCache");
            Cache<String, Connection> connectionCache = (Cache<String, Connection>) fieldConnectionCache.get(this);
            ConcurrentMap<String, Connection> cacheAsMap = connectionCache.asMap();
            if (cacheAsMap != null) {
                int randomId = new Random().nextInt(cacheAsMap.size());
                Connection loadedConn = cacheAsMap.get(cacheAsMap.keySet().toArray()[randomId]);
                return loadedConn;
            }

        } catch (IllegalAccessException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Field getField(String fieldName) throws NoSuchFieldException {
        Field fieldUrl = JdbcMeta.class.getDeclaredField(fieldName);
        fieldUrl.setAccessible(true);
        return fieldUrl;
    }
}
