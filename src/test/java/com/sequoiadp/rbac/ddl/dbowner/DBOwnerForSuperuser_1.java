package com.sequoiadp.rbac.ddl.dbowner;

import java.sql.Connection;

import java.sql.SQLException;
import java.sql.Statement;
import org.testng.annotations.Test;
import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;

/*
 * @Description   : Database Ownership Verification for super user
 * @Author        : Lena
 */
public class DBOwnerForSuperuser_1 extends SDPTestBase {

    @Test
    public void test() throws SQLException, InterruptedException {
        Connection conn1 = null;
        Statement st1 = null;
        try {
        	//管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            
            String descsql = "desc database " + getConfig("dbName");
            st1.executeQuery(descsql);           
        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
			st1.close();
            conn1.close();
        }
    }
}
