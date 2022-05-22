package com.mc.test;

import com.revinate.guava.util.concurrent.RateLimiter;
import org.h2.jdbcx.JdbcConnectionPool;

import java.sql.*;
import java.util.UUID;

import static java.lang.System.exit;

public class H2BenchMarkTest {
    public static void main(String [] args) throws Exception{
        if(args.length != 1){
            System.out.println("usage:com.mc.test.H2BenchMarkTest <insertspersecond>");
            exit(1);
        }
        Class.forName("org.h2.Driver");
        JdbcConnectionPool cp = JdbcConnectionPool.create("jdbc:h2:~/benchmark;CACHE_SIZE=0","","");
        Statement stmt = cp.getConnection().createStatement();
        String data = "H2 is a relational database management system written in Java. It can be embedded in Java applications or run in client-server mode.";
        stmt.execute("create table if not exists att (id varchar(48), key varchar(256), value blob)");
        String query = "insert into att (id,key,value) values(?,?,?,?)";
        String rate = args[0];

        RateLimiter rateLimiter = RateLimiter.create(Integer.parseInt(rate));
        int i=0;
        long startTime= System.currentTimeMillis();
        while(true){
            rateLimiter.acquire();
            Connection conn = cp.getConnection();
            PreparedStatement deltaPreparedStatement = conn.prepareStatement(query);
            deltaPreparedStatement.setString(1, UUID.randomUUID().toString());
            deltaPreparedStatement.setString(2, "test");
            deltaPreparedStatement.setString(3, "key");
            Blob deltaBlob = conn.createBlob();
            deltaBlob.setBytes(1,data.getBytes());
            deltaPreparedStatement.setBlob(4, deltaBlob);
            deltaPreparedStatement.executeUpdate();
            deltaPreparedStatement.close();

            conn.commit();
            conn.close();
            i++;
            long endTime=System.currentTimeMillis();
            if(endTime - startTime > 60000){
                System.out.println("Inserted " + i +" records");
                startTime = endTime;
                i=0;
                ResultSet rs = stmt.executeQuery("select count(*) from att");
                while(rs.next()) {
                    System.out.println("total records " + rs.getInt(1));
                }
            }
        }
    }
}
