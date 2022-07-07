package com.sequoiadp.rbac.ownership;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.ParaBeen;
import com.sequoiadp.testcommon.TestBase;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class AfterDrop354_356 extends TestBase {
    private Connection adminConn, testConn;
    private Statement adminStmt, testStmt;

    @BeforeClass
    public void initConn() throws SQLException {
        adminConn = HiveConnection.getInstance().getAdminConnect();
        adminStmt = adminConn.createStatement();
        testConn = HiveConnection.getInstance().getTestConnect();
        testStmt = testConn.createStatement();

        String s3 = "s3a://"+ ParaBeen.getConfig("S3Bucket")+'/' ;

        adminStmt.execute("create database if not exists dropobj");
        adminStmt.execute("create table dropobj.t1(id int) using delta location \""+s3+"ownt1\"");
        adminStmt.execute("alter table dropobj.t1 owner to user "+ParaBeen.getConfig("testUser"));

        try {
            adminStmt.execute("drop group g1");
        } catch (SQLException e){
            //如果不能删除group视为无事发生
        }
    }

    @Test
    public void test_354(){

    }

    @Test
    public void test_356() throws SQLException {
        adminStmt.execute("create group g1");
        adminStmt.execute("grant select on table dropobj.t1 to group g1");
        adminStmt.execute("grant modify on table dropobj.t1 to group g1");
        adminStmt.execute("grant all on table dropobj.v1 to group g1");
        adminStmt.execute("drop group g1");
        adminStmt.execute("create group g1");
        Assert.assertEquals(getGroupSet("g1").next(),false);
        adminStmt.execute("drop group g1");
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
        String drop="drop database dropobj cascade";
        adminStmt.execute(drop);
        adminStmt.close();
        adminConn.close();
        testStmt.close();
        testConn.close();
    }
}
