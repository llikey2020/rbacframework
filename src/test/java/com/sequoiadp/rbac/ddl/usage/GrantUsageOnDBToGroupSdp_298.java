package com.sequoiadp.rbac.ddl.usage;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;

/*
 * @Description   : GRANT USAGE ON DATABASE to GROUP
 * @Author        : Lena
 */
public class GrantUsageOnDBToGroupSdp_298 extends SDPTestBase {

    public GrantUsageOnDBToGroupSdp_298() {
        super.notUsage();
        super.hasGroup();
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
            
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", getConfig("testUser"));
            st1.executeQuery(addgpusersql);

            String grantsql = HiveConnection.getInstance().grantSql("usage","database",getConfig("dbName"),"group",getConfig("testGroup"));
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
            st2.close();
            conn1.close();
            conn2.close();
        }
    }
}
