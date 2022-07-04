package com.sequoiadp.rbac.show;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.ParaBeen;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class ShowGrant_329_332 {
    private Connection adminConn, testConn;
    private Statement adminStmt, testStmt;
    private static String showgrant_base="show grants";

    @BeforeClass
    public void initConn() throws SQLException {
        adminConn = HiveConnection.getInstance().getAdminConnect();
        adminStmt = adminConn.createStatement();
        testConn = HiveConnection.getInstance().getTestConnect();
        testStmt = testConn.createStatement();

        String s3 = "s3a://"+ ParaBeen.getConfig("S3Bucket")+'/' ;

        adminStmt.execute("create database showobj");
        adminStmt.execute("create table showobj.t1(id int) using delta location "+s3+"showt1");
        adminStmt.execute("grant all on database to user test");
    }

    @Test
    public void test_329() throws SQLException {
        testStmt.executeQuery(showgrant_base);
    }

    @Test
    public void test_330() throws SQLException {
        adminStmt.executeQuery(showgrant_base+" for user test");
    }

    @Test
    public void test_331() throws SQLException {
        testStmt.executeQuery(showgrant_base+" for user test");
    }

//    @Test
    public void test_332() throws SQLException {
        testStmt.executeQuery(showgrant_base);
    }
    @AfterClass
    public void closeConn() throws SQLException {
        String drop="drop database showobj cascade";
        adminStmt.execute(drop);
        adminStmt.close();
        adminConn.close();
        testStmt.close();
        testConn.close();
    }
}
