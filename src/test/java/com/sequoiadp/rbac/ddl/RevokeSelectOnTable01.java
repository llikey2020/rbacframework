package com.sequoiadp.rbac.ddl;/*
 * @Description   :
 * @Author        :  Fangjun
 * @CreateTime    : 2022/6/21 21:54
 * @LastEditTime  : 2022/6/21 21:54
 * @LastEditors   :
 */

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;

import java.sql.*;

public class RevokeSelectOnTable01 extends SDPTestBase {


    public RevokeSelectOnTable01(){
        super.setTableName("revokeselectontable01");
    }

    @Test(expectedExceptions = SQLException.class,expectedExceptionsMessageRegExp = ".* does not have select privilege on .*")
    public void Test() throws SQLException {
        Connection conn1 = null, conn2 = null;
        Statement st1 = null, st2 = null;
        try {
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1 = conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);


            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn1.createStatement();
            st2.executeQuery(usagesql);


            String grantselectsql = HiveConnection.getInstance().grantSql("select", "table", tableName, "user", getConfig("testUser"));
            st1.executeQuery(grantselectsql);


            //select
            String selectsql = HiveConnection.getInstance().selectTv(SDPTestBase.getConfig("dbName"),tableName);
            System.out.println(selectsql);
            PreparedStatement stmt1 = conn2.prepareStatement(selectsql );
            stmt1.executeQuery();


            String revokesql = HiveConnection.getInstance().revokeSql("select","table",tableName,"user",getConfig("testUser"));
            st1.executeQuery(revokesql);

            stmt1.executeQuery();

        } catch (SQLException  e) {
            e.printStackTrace();
            throw e;
        }  catch (Exception e) {
            e.printStackTrace();
        } finally {
            st2.close();
            st1.close();
            conn1.close();
            conn2.close();
        }
    }
}
