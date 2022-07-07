package com.sequoiadp.rbac.ownership;

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

public class GrantOwner351_352 extends TestBase {
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
        adminStmt.execute("grant usage on database ownobj to user "+ParaBeen.getConfig("testUser"));
        adminStmt.execute("alter table ownobj.t1 owner to user "+ParaBeen.getConfig("testUser"));
        adminStmt.execute("alter view ownobj.v1 owner to user "+ParaBeen.getConfig("testUser"));
    }

    @Test
    public void test_351() throws SQLException {
        testStmt.execute("alter table ownobj.t1 owner to user "+ParaBeen.getConfig("rootUser"));
        GeneralComparison.findInResult(getUserSet(ParaBeen.getConfig("rootUser")),new int[]{3,4},new String[]{"/ownobj/t1","OWNER"});

        adminStmt.execute("alter table ownobj.t1 owner to user "+ParaBeen.getConfig("testUser"));

        testStmt.execute("alter table ownobj.t1 owner to group another");
        GeneralComparison.findInResult(getGroupSet("another"),new int[]{3,4},new String[]{"/ownobj/t1","OWNER"});
    }

    @Test
    public void test_352() throws SQLException {
        testStmt.execute("alter view ownobj.v1 owner to user "+ParaBeen.getConfig("rootUser"));
        GeneralComparison.findInResult(getUserSet(ParaBeen.getConfig("rootUser")),new int[]{3,4},new String[]{"/ownobj/v1","OWNER"});

        adminStmt.execute("alter view ownobj.v1 owner to user "+ParaBeen.getConfig("testUser"));

        testStmt.execute("alter view ownobj.v1 owner to group another");
        GeneralComparison.findInResult(getGroupSet("another"),new int[]{3,4},new String[]{"/ownobj/v1","OWNER"});
    }

    /**
     *
     */
    private ResultSet getUserSet(String user) throws SQLException {
        return adminStmt.executeQuery("show grants for user "+user);
    }
    private ResultSet getGroupSet(String group) throws SQLException {
        return adminStmt.executeQuery("show grants for group "+group);
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
