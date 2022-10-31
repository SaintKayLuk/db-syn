package com.security.database;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * @author Saint Kay
 * @date 2022/1/13
 * mysql和clickhouse的数据源
 */
@Configuration
public class MyJdbcTemplate {


    @Bean(name = "mysqlDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.mysql")
    public DataSource mysqldataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "clickhouseDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.clickhouse")
    public DataSource clickhousedataSource() {
        return DataSourceBuilder.create().build();
    }


    @Bean(name = "mysqlJdbcTemplate")
    public JdbcTemplate mysqlJdbcTemplate(@Qualifier("mysqlDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = "clickhouseJdbcTemplate")
    public JdbcTemplate secondaryJdbcTemplate(@Qualifier("clickhouseDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }


}
