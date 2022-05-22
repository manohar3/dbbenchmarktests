package com.mc.benchmark.db;

import com.mc.benchmark.util.InsertUtil;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;

import java.sql.Connection;
import java.util.Properties;


//docker run -p 5555:5432  --name postgres -e POSTGRES_DB=cg -e POSTGRES_USER=dbadmin -e POSTGRES_PASSWORD=dbadmin -d postgres

public class PostgresBenchMarkTest {
    public static void main(String [] args) throws Throwable {
        if(args.length != 2){
            System.out.println("usage:PostgresBenchMarkTest <insertspersecond> <mode>");
            System.exit(1);
        }
        Properties dbproperties = new Properties();
        dbproperties.put("username","dbadmin");
        dbproperties.put("password","dbadmin");
        dbproperties.put("url","jdbc:postgresql://localhost:5555/cg");
        dbproperties.put("driverClassName","org.postgresql.Driver");
        dbproperties.put("initialSize", 1);
        dbproperties.put("defaultAutoCommit", false);
        BasicDataSource source = BasicDataSourceFactory.createDataSource(dbproperties);
        Connection conn = source.getConnection();
        InsertUtil.insertRecords(conn, args[0], args[1]);
    }
}
