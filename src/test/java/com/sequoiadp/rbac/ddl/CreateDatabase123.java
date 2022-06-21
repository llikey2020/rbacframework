package com.sequoiadp.rbac.ddl;
/*
 * @Description   : sequoiadb grant test create privilege to test and verify
 * @Author        :  Fangjun
 * @CreateTime    : 2022/6/1 18:04
 * @LastEditTime  : 2022/6/1 18:04
 * @LastEditors   :
 */

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.ParaBeen;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateDatabase123 {
    protected static String dbName = "ctestdb";
    @Test
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
        ResultSet rs = null;
        try{
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1 = conn1.createStatement();
            String grantcreatesql = HiveConnection.getInstance().grantSql("create","database", dbName,"user", ParaBeen.getConfig("testUser"));
            st1.executeQuery(grantcreatesql);

            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();
            String  createsql = "create database " +dbName + ";";
            st2.executeQuery(createsql);
            String showsql = "show databases;";
            rs = st2.executeQuery(showsql);

            String dropsql = HiveConnection.getInstance().dropSql("database",dbName);
            st1.executeQuery(dropsql);
        }
        finally {
            rs.close();
            st1.close();
            st2.close();
            conn1.close();
            conn2.close();
        }
    }
}
