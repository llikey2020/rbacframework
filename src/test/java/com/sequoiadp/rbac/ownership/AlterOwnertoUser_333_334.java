package com.sequoiadp.rbac.ownership;

import com.sequoiadp.testcommon.GeneralComparison;
import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.ParaBeen;
import com.sequoiadp.testcommon.TestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class AlterOwnertoUser_333_334 extends TestBase {
    private Connection adminConn, testConn;
    private Statement adminStmt, testStmt;

    @BeforeClass
    public void initConn() throws SQLException {
        adminConn = HiveConnection.getInstance().getAdminConnect();
        adminStmt = adminConn.createStatement();
        testConn = HiveConnection.getInstance().getTestConnect();
        testStmt = testConn.createStatement();

        String s3 = "s3a://"+ ParaBeen.getConfig("S3Bucket")+'/' ;

        adminStmt.execute("create database if not exists ownobj");
        adminStmt.execute("create table ownobj.t1(id int) using delta location \""+s3+"ownt1\"");
        adminStmt.execute("create view ownobj.v1 as select * from ownobj.t1");
        adminStmt.execute("alter table ownobj.t1 owner to user "+ParaBeen.getConfig("testUser"));
        adminStmt.execute("alter view ownobj.v1 owner to user "+ParaBeen.getConfig("testUser"));
        adminStmt.execute("grant usage on database ownobj to user "+ParaBeen.getConfig("testUser"));
    }

    @Test
    public void test_333() throws Exception {
        GeneralComparison.findInResult(testStmt.executeQuery("show grants"),new int[]{2,3},new String[]{"/ownobj/t1","OWNER"});
    }

    @Test
    public void test_334() throws Exception {
        GeneralComparison.findInResult(testStmt.executeQuery("show grants"),new int[]{2,3},new String[]{"/ownobj/v1","OWNER"});
    }

    @AfterClass
    public void closeConn() throws SQLException {
        String drop="drop database ownobj cascade";
        adminStmt.execute(drop);
        adminStmt.close();
        adminConn.close();
        testStmt.close();
        testConn.close();
    }
}
