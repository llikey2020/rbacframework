package com.sequoiadp.rbac.show;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.ParaBeen;
import com.sequoiadp.testcommon.TestBase;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class ShowObject313_315 extends TestBase {
    private Connection adminConn, testConn;
    private Statement adminStmt, testStmt;

    @BeforeClass
    public void initConn() throws SQLException {
        adminConn = HiveConnection.getInstance().getAdminConnect();
        adminStmt = adminConn.createStatement();
        testConn = HiveConnection.getInstance().getTestConnect();
        testStmt = testConn.createStatement();

        String s3 = "s3a://"+ ParaBeen.getConfig("S3Bucket")+'/' ;

        adminStmt.execute("create database if not exists showobj");
        adminStmt.execute("create table showobj.t1(id int) using delta location \""+s3+"showt1\"");
        adminStmt.execute("create view showobj.v1 as select * from showobj.t1");
        adminStmt.execute("grant usage on database showobj to user "+ParaBeen.getConfig("testUser"));
    }

    @Test
    public void test_313() throws SQLException {
        String show="show create table showobj.t1",
                tempgrant="grant read_metadata on table showobj.t1 to user "+ParaBeen.getConfig("testUser");
        adminStmt.execute(tempgrant);
        Assert.assertThrows(SQLException.class, () -> {
            testStmt.execute(show);
        });
    }

    @Test(skipFailedInvocations = true)
    public void test_314() throws SQLException {
        String show="show databases ";
        testStmt.execute(show);
    }

    @Test
    public void test_315() throws SQLException {
        String show="show create view default.v1",
        tempgrant="grant read_metadata on view showobj.v1 to user test";
        adminStmt.execute(tempgrant);
        Assert.assertThrows(SQLException.class, () -> {
            testStmt.execute(show);
        });
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
