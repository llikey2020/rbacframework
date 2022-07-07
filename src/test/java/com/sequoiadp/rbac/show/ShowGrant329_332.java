package com.sequoiadp.rbac.show;

import com.sequoiadp.testcommon.GeneralComparison;
import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.ParaBeen;
import com.sequoiadp.testcommon.TestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ShowGrant329_332 extends TestBase {
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

        adminStmt.execute("create database if not exists showobj");
        adminStmt.execute("create table showobj.t1(id int) using delta location \""+s3+"showt1\"");
        adminStmt.execute("grant all on database showobj to user "+ParaBeen.getConfig("testUser"));
        adminStmt.execute("grant usage on database showobj to user "+ParaBeen.getConfig("testUser"));
        adminStmt.execute("grant all on database showobj to group "+ParaBeen.getConfig("testGroup"));
        adminStmt.execute("grant usage on database showobj to group "+ParaBeen.getConfig("testGroup"));
    }

    @Test
    public void test_329() throws SQLException {
        ResultSet set=testStmt.executeQuery(showgrant_base);
        GeneralComparison.findInResult(set,new int[]{2,3},new String[]{"/showobj/","ALL"});
        GeneralComparison.findInResult(set,new int[]{2,3},new String[]{"/showobj/","USAGE"});
    }

    @Test
    public void test_330() throws SQLException {
        ResultSet set=adminStmt.executeQuery(showgrant_base+" for user "+ParaBeen.getConfig("testUser"));
        GeneralComparison.findInResult(set,new int[]{3,4},new String[]{"/showobj/","ALL"});
        GeneralComparison.findInResult(set,new int[]{3,4},new String[]{"/showobj/","USAGE"});
    }

    @Test
    public void test_331() throws SQLException {
        ResultSet set=adminStmt.executeQuery(showgrant_base+" for group "+ParaBeen.getConfig("testGroup"));
        GeneralComparison.findInResult(set,new int[]{3,4},new String[]{"/showobj/","ALL"});
        GeneralComparison.findInResult(set,new int[]{3,4},new String[]{"/showobj/","USAGE"});
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
