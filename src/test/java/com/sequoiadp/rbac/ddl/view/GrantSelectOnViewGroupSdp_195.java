package com.sequoiadp.rbac.ddl.view;


import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import com.sequoiadp.testcommon.SDPViewTestBase;

import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : sequoiadb grant testuser select privilege on a view to test and verify
 * @Author        : Lena
 */

public class GrantSelectOnViewGroupSdp_195 extends SDPViewTestBase {
    public GrantSelectOnViewGroupSdp_195() {
        super.setTableName("tablea");
        super.setViewName(this.getTableName() + "_view");
        super.hasGroup();
    }
    //测试点
    @Test
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", getConfig("testUser"));
            st1.executeQuery(addgpusersql);
            
            String grantsqlview = HiveConnection.getInstance().grantSql("select","view",viewName,"group",getConfig("testGroup"));
            st1.executeQuery(grantsqlview);
            
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
            st2.close();
            conn1.close();
            conn2.close();
        }
    }
}

