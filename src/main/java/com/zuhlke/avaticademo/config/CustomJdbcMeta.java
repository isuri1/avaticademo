package com.zuhlke.avaticademo.config;

import com.google.common.cache.Cache;
import org.apache.calcite.avatica.jdbc.JdbcMeta;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

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

        Connection connection = null;
        try {
            connection = dataSource.getConnection();

        } catch (SQLException ex) {
            try {
                connection = getSharedConnection();
            } catch (IllegalAccessException | NoSuchFieldException e) {
                e.printStackTrace();
            }
            return connection;
        }
        return connection;
    }

    private Connection getSharedConnection() throws IllegalAccessException, NoSuchFieldException {
        Field fieldConnectionCache;

        fieldConnectionCache = getField("connectionCache");
        Cache<String, Connection> connectionCache = (Cache<String, Connection>) fieldConnectionCache.get(this);
        ConcurrentMap<String, Connection> cacheAsMap = connectionCache.asMap();

        if (cacheAsMap != null && !cacheAsMap.isEmpty()) {
            PriorityQueue<Map.Entry<Connection, Long>> pq = rankConnections(cacheAsMap);
            Connection connection = Objects.requireNonNull(pq.peek()).getKey();
            return connection;
        }
        return null;
    }

    private PriorityQueue<Map.Entry<Connection, Long>> rankConnections(ConcurrentMap<String, Connection> cacheAsMap) {
        final Map<Connection, Long> connectionCountMap = cacheAsMap.values().stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        final Set<Map.Entry<Connection, Long>> entries = connectionCountMap.entrySet();
        PriorityQueue<Map.Entry<Connection, Long>> pq =
                new PriorityQueue<>(Comparator.comparingLong(Map.Entry::getValue));
        pq.addAll(entries);
        return pq;
    }

    @Override
    public void closeConnection(ConnectionHandle ch) {
        Field fieldConnectionCache;
        try {

            fieldConnectionCache = getField("connectionCache");
            Cache<String, Connection> connectionCache = (Cache<String, Connection>) fieldConnectionCache.get(this);
            Connection conn = connectionCache.getIfPresent(ch.id);
            if (conn == null) {
                return;
            }
            connectionCache.invalidate(ch.id);

            final ConcurrentMap<String, Connection> cacheAsMap = connectionCache.asMap();
            boolean isShared = isSharedConnection(conn, cacheAsMap);

            if (!isShared) conn.close();

        } catch (IllegalAccessException | NoSuchFieldException | SQLException e) {
            e.printStackTrace();
        }
    }

    private boolean isSharedConnection(Connection conn, ConcurrentMap<String, Connection> cacheAsMap) {
        boolean isSharedConn = false;
        for (Connection connection : cacheAsMap.values()) {
            if (Objects.equals(connection, conn)) {
                isSharedConn = true;
                break;
            }
        }
        return isSharedConn;
    }

    private Field getField(String fieldName) throws NoSuchFieldException {
        Field fieldUrl = JdbcMeta.class.getDeclaredField(fieldName);
        fieldUrl.setAccessible(true);
        return fieldUrl;
    }
}
