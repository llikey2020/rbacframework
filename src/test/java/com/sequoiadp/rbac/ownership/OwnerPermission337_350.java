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

public class OwnerPermission337_350 extends TestBase {
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

    @Test(priority = 0)
    public void test_339() throws SQLException {
        testStmt.execute("grant select on table ownobj.t1 to user another");
        GeneralComparison.findInResult(getUserSet(),new int[]{3,4},new String[]{"/ownobj/t1","SELECT"});
    }

    @Test(priority = 1)
    public void test_340() throws SQLException {
        testStmt.execute("grant select on table ownobj.t1 to group another");
        GeneralComparison.findInResult(getGroupSet(),new int[]{3,4},new String[]{"/ownobj/t1","SELECT"});
    }

    @Test(priority = 2)
    public void test_341() throws SQLException {
        testStmt.execute("grant select on view ownobj.v1 to user another");
        GeneralComparison.findInResult(getUserSet(),new int[]{3,4},new String[]{"/ownobj/v1","SELECT"});
    }

    @Test(priority = 3)
    public void test_342() throws SQLException {
        testStmt.execute("grant select on view ownobj.v1 to group another");
        GeneralComparison.findInResult(getGroupSet(),new int[]{3,4},new String[]{"/ownobj/v1","SELECT"});
    }

    @Test(priority = 4)
    public void test_343() throws SQLException {
        testStmt.execute("grant create on view ownobj.v1 to user another");
        GeneralComparison.findInResult(getUserSet(),new int[]{3,4},new String[]{"/ownobj/v1","CREATE"});
    }

    @Test(priority = 5)
    public void test_344() throws SQLException {
        testStmt.execute("grant create on view ownobj.v1 to group another");
        GeneralComparison.findInResult(getGroupSet(),new int[]{3,4},new String[]{"/ownobj/v1","CREATE"});
    }

    @Test(priority = 6)
    public void test_345() throws SQLException {
        testStmt.execute("grant modify on table ownobj.t1 to user another");
        GeneralComparison.findInResult(getUserSet(),new int[]{3,4},new String[]{"/ownobj/t1","MODIFT"});
    }

    @Test(priority = 7)
    public void test_346() throws SQLException {
        testStmt.execute("grant modify on table ownobj.t1 to group another");
        GeneralComparison.findInResult(getGroupSet(),new int[]{3,4},new String[]{"/ownobj/t1","MODIFT"});
    }

    @Test(priority = 8)
    public void test_347() throws SQLException {
        testStmt.execute("grant all on table ownobj.t1 to user another");
        GeneralComparison.findInResult(getUserSet(),new int[]{3,4},new String[]{"/ownobj/t1","ALL"});
    }

    @Test(priority = 9)
    public void test_348() throws SQLException {
        testStmt.execute("grant all on table ownobj.t1 to group another");
        GeneralComparison.findInResult(getGroupSet(),new int[]{3,4},new String[]{"/ownobj/t1","ALL"});
    }

    @Test(priority = 10)
    public void test_349() throws SQLException {
        testStmt.execute("grant all on view ownobj.v1 to user another");
        GeneralComparison.findInResult(getUserSet(),new int[]{3,4},new String[]{"/ownobj/v1","ALL"});
    }

    @Test(priority = 11)
    public void test_350() throws SQLException {
        testStmt.execute("grant all on table ownobj.t1 to group another");
        GeneralComparison.findInResult(getGroupSet(),new int[]{3,4},new String[]{"/ownobj/v1","ALL"});
    }

    /**
     * 统一验证从用例339到用例350的
     */
    private ResultSet getUserSet() throws SQLException {
        return adminStmt.executeQuery("show grants for user another");
    }
    private ResultSet getGroupSet() throws SQLException {
        return adminStmt.executeQuery("show grants for group another");
    }

    @Test(priority = 13)
    public void test_337() throws SQLException {
        testStmt.execute("drop table ownobj.t1");
    }

    @Test(priority = 14)
    public void test_338() throws SQLException {
        testStmt.execute("drop view ownobj.v1");
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
