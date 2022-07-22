package com.sequoiadp.rbac.ddl.create;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;

/*
 * @Description   : Super user GRANT CREATE ON DATABASE to GROUP
 * @Author        : Lena
 */
public class GrantCreateDBToGroupSdp_232 extends SDPTestBase {
	
    public GrantCreateDBToGroupSdp_232() {
        super.hasGroup();
    }
    public static final String DBNAME = "newdbname";
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

            String grantsql = HiveConnection.getInstance().grantSql("create","database",DBNAME,"group",getConfig("testGroup"));
            st1.executeQuery(grantsql);
                        
            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();            

            String createdbsql = "create database " + DBNAME;
            st2.executeQuery(createdbsql);
            
            String dropdbsql = HiveConnection.getInstance().dropSql("database",DBNAME );
            st1.executeQuery(dropdbsql);

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
