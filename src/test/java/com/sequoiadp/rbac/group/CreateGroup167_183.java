package com.sequoiadp.rbac.group;

import com.sequoiadp.testcommon.GeneralComparison;
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

@Test(groups = "group")
public class CreateGroup167_183 extends TestBase {
    private Connection adminConn, testConn;
    private Statement adminStmt, testStmt;
    private String groupname="dev",normalCreate="create group "+groupname,normalDrop="drop group "+groupname;

    @BeforeClass
    public void initConn() throws SQLException {
        adminConn = HiveConnection.getInstance().getAdminConnect();
        adminStmt = adminConn.createStatement();
        testConn = HiveConnection.getInstance().getTestConnect();
        testStmt = testConn.createStatement();
        try {
            runGroupTestByAdmin(normalDrop);
        } catch (SQLException e){
            //如果不能删除group视为无事发生
        }
    }

    public void test_167() throws SQLException {
        runGroupTestByAdmin(normalCreate, normalDrop);
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
            runGroupTestByUser(normalCreate);
            runGroupTestByAdmin(normalDrop);
        });
    }

    public void test_172() throws SQLException {
        String sql1 = "create group dev", sql2 = "drop group dev";
        Assert.assertThrows(SQLException.class, () -> {
            adminStmt.execute(sql1);
            adminStmt.execute(sql1);
        });
        adminStmt.execute(sql2);
    }

    public void test_173() {
        String sql1 = "create groups dev", sql2 = "drop group dev";
        Assert.assertThrows(SQLException.class, () -> {
            runGroupTestByAdmin(sql1, sql2);
        });
    }

    public void test_174() {
        String sql1 = "create group dev.dev", sql2 = "drop group dev.dev";
        Assert.assertThrows(SQLException.class, () -> {
            runGroupTestByAdmin(sql1, sql2);
        });
    }

    public void test_176() throws SQLException {
        String sql1 = "create group dev", sql2 = "drop group dev", sqladd="alter group dev add user u1";
        runGroupTestByAdmin(sql1,sql2,sqladd);
    }

    public void test_179() {
        String sql2 = "drop group none_exist";
        Assert.assertThrows(SQLException.class, () -> {
            adminStmt.execute(sql2);
        });
    }
    public void test_180_181() throws SQLException {
        String sql1 = "create group dev", sql2="grant all on database default to user "+ParaBeen.getConfig("testUser"),
                sql3 = "drop group dev",sql4="show grants for group dev",sql5="show grants for user "+ParaBeen.getConfig("testUser"),
        clean="revoke all on database default from user "+ParaBeen.getConfig("testUser");
        String[] array={"grant all on database default to group dev",
        "grant usage on database default to group dev",
        "grant create on database aaa to group dev"};
        runGroupTestByAdmin(sql1);
        runGroupTestByAdmin(sql2);
        runGroupTestByAdmin(array);

        runGroupTestByAdmin(sql3);
        Assert.assertEquals(adminStmt.executeQuery(sql4).next(),false);
        GeneralComparison.findInResult(adminStmt.executeQuery(sql5), new int[]{3, 4}, new String[]{"/default/", "ALL"});
        runGroupTestByAdmin(clean);
    }

    public void test_182() throws SQLException {
        String sql1 = "create group dev", sql2 = "drop groups dev";
        runGroupTestByAdmin(sql1);
        Assert.assertThrows(SQLException.class, () -> {
            runGroupTestByUser(sql2);
        });
        runGroupTestByAdmin("drop group dev");
    }

    public void test_183() throws SQLException {
        String sql1 = "create group dev", sql2 = "drop groups dev";
        Assert.assertThrows(SQLException.class, () -> {
            runGroupTestByAdmin(sql1,sql2);
        });
        adminStmt.execute("drop group dev");
    }

    private void runGroupTestByAdmin(String... sqlArray) throws SQLException {
        for (int i = 0; i < sqlArray.length; i++) {
            adminStmt.execute(sqlArray[i]);
        }
    }

    private void runGroupTestByUser(String... sqlArray) throws SQLException {
        for (int i = 0; i < sqlArray.length; i++) {
            testStmt.execute(sqlArray[i]);
        }
    }

    @AfterClass
    public void closeConn() throws SQLException {
        adminStmt.close();
        adminConn.close();
        testStmt.close();
        testConn.close();
    }
}
