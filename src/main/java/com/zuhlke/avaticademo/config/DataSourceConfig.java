package com.zuhlke.avaticademo.config;

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Configuration
public class DataSourceConfig {

    private static final String H2_2_USER_NAME = "sa";
    private static final String H2_2_PWD = "";

    @Bean
    public CustomJdbcMeta customJdbcMeta(@Value("${spring.datasource.hikari.jdbc-url}") String url, Properties properties, DataSource dataSource) throws SQLException {
        return new CustomJdbcMeta(url, properties, dataSource);
    }

    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.hikari")
    public DataSource dataSource(){
        return DataSourceBuilder.create().build();
    }

    @Bean
    public Properties properties(){

        Properties properties = new Properties();
        properties.put("user", H2_2_USER_NAME);
        properties.put("password", H2_2_PWD);
        properties.put(JdbcMeta.ConnectionCacheSettings.EXPIRY_DURATION.key(), String.valueOf(Integer.MAX_VALUE));
        properties.put(JdbcMeta.ConnectionCacheSettings.EXPIRY_UNIT.key(), TimeUnit.DAYS.name());

        return properties;
    }

}