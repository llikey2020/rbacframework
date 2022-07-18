package com.sequoiadp.rbac.ddl.usage;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;

/*
 * @Description   : GRANT USAGE privilege syntax validation to user 
 * @Author        : Lena
 */
public class GrantUsageOnDBToUserSyntaxSdp_299_A extends SDPTestBase {

    public GrantUsageOnDBToUserSyntaxSdp_299_A() {
        super.notUsage();
    }
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*Operation not allowed.*")
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
        try {
        	//管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql); 

            String grantsql = HiveConnection.getInstance().grantSql("usages","database",getConfig("dbName"),"user",getConfig("testUser"));
            st1.executeQuery(grantsql);
                        
            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();
            
            st2.executeQuery(usagesql);
            

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
