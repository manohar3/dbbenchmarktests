package com.mc.benchmark.db;

import com.mc.benchmark.util.InsertUtil;
import org.h2.jdbcx.JdbcConnectionPool;

import java.sql.Connection;



//To run h2 in server mode
// java -cp h2*.jar org.h2.tools.Server -tcpAllowOthers -ifNotExists
public class H2ServerModeBenchMarkTest {
    public static void main(String [] args) throws Throwable{
        if(args.length != 2){
            System.out.println("usage:com.mc.test.H2ServerBenchMarkTest <insertspersecond> <mode>");
            System.exit(1);
        }
        Class.forName("org.h2.Driver");
        JdbcConnectionPool cp = JdbcConnectionPool.create("jdbc:h2:tcp://localhost/~/serverbenchmark","","");
        Connection conn = cp.getConnection();
        InsertUtil.insertRecords(conn, args[0], args[1]);
    }
}
