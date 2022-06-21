package com.sequoiadp.rbac.ddl;
/*
 * @Description   :
 * @Author        :  Fangjun
 * @CreateTime    : 2022/6/15 15:15
 * @LastEditTime  : 2022/6/15 15:15
 * @LastEditors   :
 */

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class WithoutUsage extends SDPTestBase {
    public WithoutUsage() {
        super.setTableName("bltgnh");
        super.notUsage();
    }
    @Test(expectedExceptions = SQLException.class,expectedExceptionsMessageRegExp = ".* does not have USAGE privilege on .*")
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
        try {
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1 = conn1.createStatement();

            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);

            String grantselectsql = HiveConnection.getInstance().grantSql("select","table",tableName,"user",getConfig("testUser"));
            st1.executeQuery(grantselectsql);

            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();

            String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"),tableName);
            st2.executeQuery(selectsql);
        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            st2.close();
            conn1.close();
            conn2.close();
        }
    }
}
