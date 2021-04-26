package com.zuhlke.avaticademo.config;

import org.apache.calcite.avatica.remote.Driver;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.HttpServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class AvaticaHttpServer {

    @Autowired
    private CustomJdbcMeta jdbcMeta;

    @PostConstruct
    public void createServer() {

        LocalService service = new LocalService(jdbcMeta);

        HttpServer server = new HttpServer.Builder<>()
                .withPort(8080)
                .withHandler(service, Driver.Serialization.JSON)
                .build();
        server.start();
    }
}
