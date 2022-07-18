package com.sequoiadp.rbac.ddl.usage;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;

/*
 * @Description   : GRANT USAGE ON DATABASE to <non existing username>
 * @Author        : Lena
 */
public class GrantUsageOnDBNonexistingUserSdp_301_A extends SDPTestBase {

    public GrantUsageOnDBNonexistingUserSdp_301_A() {
        super.notUsage();
    }
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

            String grantsql = HiveConnection.getInstance().grantSql("usage","database",getConfig("dbName"),"user","nonexistinguser");
            st1.executeQuery(grantsql);          

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            conn1.close();
        }
    }
}