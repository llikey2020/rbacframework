package com.sequoiadp.rbac.group;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.ParaBeen;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Test(groups = "group")
public class CreateGroup_167_171 {
    private Connection adminConn, testConn;
    private Statement adminStmt, testStmt;

    @BeforeClass
    public void initConn() throws SQLException {
        adminConn = HiveConnection.getInstance().getAdminConnect();
        adminStmt = adminConn.createStatement();
        testConn = HiveConnection.getInstance().getTestConnect();
        testStmt = testConn.createStatement();
    }

    public void test_167() throws SQLException {
        String sql1 = "create group dev", sql2 = "drop group dev";
        runGroupTestByAdmin(sql1, sql2);
    }

    public void test_168() throws SQLException {
        String sql1 = "create group " + ParaBeen.getConfig("testUser"), sql2 = "drop group " + ParaBeen.getConfig("testUser");
        runGroupTestByAdmin(sql1, sql2);
    }

    public void test_169() throws SQLException {
        String sql1 = "create group user", sql2 = "drop group user";
        runGroupTestByAdmin(sql1, sql2);
    }

    public void test_171() {
        String sql1 = "create group dev", sql2 = "drop group dev";
        Assert.assertThrows(SQLException.class, () -> {
            runGroupTestByUser(sql1, sql2);
        });
    }

    private void runGroupTestByAdmin(String create, String drop) throws SQLException {
        adminStmt.execute(create);
        adminStmt.execute(drop);
    }

    private void runGroupTestByUser(String create, String drop) throws SQLException {
        testStmt.execute(create);
        testStmt.execute(drop);
    }

    @AfterClass
    public void closeConn() throws SQLException {
        adminStmt.close();
        adminConn.close();
        testStmt.close();
        testConn.close();
    }
}
