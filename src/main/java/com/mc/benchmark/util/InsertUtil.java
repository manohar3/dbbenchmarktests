package com.mc.benchmark.util;

import com.revinate.guava.util.concurrent.RateLimiter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Date;
import java.util.UUID;

/*PreparedStatement diffPreparedStatement = conn.prepareStatement(query);
         diffPreparedStatement.setString(1, UUID.randomUUID().toString());
         diffPreparedStatement.setString(2, "urn:sentinel:attachment:provider:cg");
         diffPreparedStatement.setString(3, "DIFF");
         Blob diffBlob = conn.createBlob();
         diffBlob.setBytes(1,attachment.getBytes());
         diffPreparedStatement.setBlob(4, diffBlob);
         diffPreparedStatement.executeUpdate();
         diffPreparedStatement.close();
         */
public class InsertUtil {

    public static void insertRecords(Connection conn, String eps, String mode) throws Throwable{
        System.out.println(MessageFormat.format("Testing in {0} mode, eps {1}", new Object[]{mode, eps}));
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        String attachment = "{\"schema\":[{\"iqcimSetting\":\"string\"},{\"iqcimAction\":\"string\"},{\"iqcimChanges\":\"table\"}],\"rows\":[[\"/dummy/test.txt\",\"write(2)\",{\"schema\":[],\"rows\":[]}]]}";
        stmt.execute("create table if not exists att (id varchar(48), p  varchar(256), k varchar(256), v bytea)");
        stmt.close();
        String query = "insert into att (id,p,k,v) values(?,?,?,?)";
        RateLimiter rateLimiter = RateLimiter.create(Integer.parseInt(eps));
        int i=0;
        long startTime= System.currentTimeMillis();
        PreparedStatement deltaPreparedStatement = conn.prepareStatement(query);
        while(true) {
            rateLimiter.acquire();
            deltaPreparedStatement.setString(1, UUID.randomUUID().toString());
            deltaPreparedStatement.setString(2, "urn:sentinel:attachment:provider:cg");
            deltaPreparedStatement.setString(3, "DELTA");
            deltaPreparedStatement.setBytes(4, attachment.getBytes());
            if(mode.equals("batch")) {
                deltaPreparedStatement.addBatch();
                if (i % 1000 == 0) {
                    deltaPreparedStatement.executeBatch();
                    conn.commit();
                }                            }
            else {
                deltaPreparedStatement.execute();
                conn.commit();
            }
            i++;
            long endTime=System.currentTimeMillis();
            if(endTime - startTime > 60000){
                Date d = new Date();
                System.out.println( d +" Inserted " + i +" records");
                startTime = endTime;
                i=0;
            //    stmt = conn.createStatement();
             //   ResultSet rs = stmt.executeQuery("select count(*) from att");
              //  while(rs.next()) {
               //     System.out.println("total records " + rs.getInt(1));
               // }
               // rs.close();
               // stmt.close();
            }
        }
    }
}
