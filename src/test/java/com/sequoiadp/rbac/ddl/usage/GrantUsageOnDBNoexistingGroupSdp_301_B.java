package com.sequoiadp.rbac.ddl.usage;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;

/*
 * @Description   : GRANT USAGE ON DATABASE to <non existing groupname>
 * @Author        : Lena
 */
public class GrantUsageOnDBNoexistingGroupSdp_301_B extends SDPTestBase {

    public GrantUsageOnDBNoexistingGroupSdp_301_B() {
        super.notUsage();
        super.hasGroup();
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
            
            String grantsql = HiveConnection.getInstance().grantSql("usage","database",getConfig("dbName"),"group","nonexistinggroup");
            st1.executeQuery(grantsql);
            
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", "nonexistinggroup");
            st1.executeQuery(addgpusersql);
                        
            //测试用户test来验证管理员的语句
//            conn2 = HiveConnection.getInstance().getTestConnect();
//            st2 = conn2.createStatement();
//            
//            st2.executeQuery(usagesql);
            

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
			st1.close();
			if (st2 != null)
				st2.close();
			conn1.close();
			if (conn2 != null)
				conn2.close();
        }
    }
}