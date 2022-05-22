package com.mc.benchmark.db;

import com.mc.benchmark.util.InsertUtil;
import org.h2.jdbcx.JdbcConnectionPool;

import java.sql.Connection;

public class H2EmbededModeBenchMarkTest {
    public static void main(String [] args) throws Throwable{
        if(args.length != 2){
            System.out.println("usage:com.mc.test.H2BenchMarkTest <insertspersecond> <mode>");
            System.exit(1);
        }
        Class.forName("org.h2.Driver");
        JdbcConnectionPool cp = JdbcConnectionPool.create("jdbc:h2:~/benchmark;CACHE_SIZE=0","","");
        Connection conn = cp.getConnection();
        InsertUtil.insertRecords(conn, args[0], args[1]);
    }
}
