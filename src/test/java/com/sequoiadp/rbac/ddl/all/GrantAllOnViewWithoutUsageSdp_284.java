package com.sequoiadp.rbac.ddl.all;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPViewTestBase;
import org.testng.annotations.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT ALL ON VIEW without granting usage to user
 * @Author        : Lena
 */

public class GrantAllOnViewWithoutUsageSdp_284 extends SDPViewTestBase {
    public GrantAllOnViewWithoutUsageSdp_284() {
        super.setTableName("tablea");
        super.setViewName(this.getTableName() + "_vIew");
        super.notUsage();
    }
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*does not have usage privilege on.*")
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String grantsqlview = HiveConnection.getInstance().grantSql("all","view",viewName,"user",getConfig("testUser"));
            st1.executeQuery(grantsqlview);
            
            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();;

           st2.executeQuery(usagesql);
            
            String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"),viewName);
            st2.executeQuery(selectsql);

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            if(st2 != null) st2.close();
            conn1.close();
            if(conn2 != null) conn2.close();
        }
    }
}