package com.sequoiadp.rbac.ddl.view;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPViewTestBase;

import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : non owner GRANT SELECT ON VIEW TO USER
 * @Author        : Lena
 */


public class NoownerGrantSelectViewSdp_200_A extends SDPViewTestBase {
    public NoownerGrantSelectViewSdp_200_A() {
        super.setTableName("tablea");
        super.setViewName(this.getTableName() + "_view");
       
    }
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*not have grant privilege on view.*")
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null, conn3 = null;
        Statement st1 = null,st2 = null, st3 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String grantsql = HiveConnection.getInstance().grantSql("usage","database",getConfig("dbName"),"user",getConfig("nonowner"));
            st1.executeQuery(grantsql);
                       
            conn3 = HiveConnection.getInstance().getNonownerConnect();
            st3= conn3.createStatement();
            st3.executeQuery(usagesql);  
            String grantsqlview = HiveConnection.getInstance().grantSql("select","view",viewName,"user",getConfig("testUser"));
            st3.executeQuery(grantsqlview);
            
            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();
            String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"),viewName);
            st2.executeQuery(selectsql);

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            if(st2 != null)st2.close();
            st3.close();
            conn1.close();
            if(conn2 != null)conn2.close();
            conn3.close();
        }
    }
}

