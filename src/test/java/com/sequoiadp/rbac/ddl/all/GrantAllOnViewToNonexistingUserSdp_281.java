
package com.sequoiadp.rbac.ddl.all;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPViewTestBase;
import org.testng.annotations.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT ALL ON VIEW TO USER <non existing username> 
 * @Author        : Lena
 */

public class GrantAllOnViewToNonexistingUserSdp_281 extends SDPViewTestBase {
    public GrantAllOnViewToNonexistingUserSdp_281() {
        super.setTableName("tablea");
        super.setViewName(this.getTableName() + "_vIew");
    }
    //测试点
    @Test
    public void test() throws SQLException {
        Connection conn1 = null;
        Statement st1 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String grantsqlview = HiveConnection.getInstance().grantSql("all","view",viewName,"user","nonexistinguser");
            st1.executeQuery(grantsqlview);
            
//            //测试用户test来验证管理员的语句
//            conn2 = HiveConnection.getInstance().getTestConnect();
//            st2 = conn2.createStatement();;
//
//            st2.executeQuery(usagesql);
//            
//            String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"),viewName);
//            st2.executeQuery(selectsql);

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            conn1.close();
        }
    }
}

