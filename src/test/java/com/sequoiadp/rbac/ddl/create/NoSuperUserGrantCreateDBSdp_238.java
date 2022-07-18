package com.sequoiadp.rbac.ddl.create;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;

/*
 * @Description   : Non super user GRANT CREATE ON DATABASE to user
 * @Author        : Lena
 */
public class NoSuperUserGrantCreateDBSdp_238 extends SDPTestBase {
    public static final String DBNAME = "newdbname";
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*does not have grant privilege on database.*")
    public void test() throws SQLException {
        Connection conn1 = null;
        Statement st1 = null;
        Connection conn2 = null;
        Statement st2 = null;
        try {
                        
            //测试用户test来验证管理员的语句
            conn1 = HiveConnection.getInstance().getNonownerConnect();
            st1 = conn1.createStatement();
            
            String grantsql = HiveConnection.getInstance().grantSql("create","database",DBNAME,"user",getConfig("testUser"));
            st1.executeQuery(grantsql);
            

            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();
            String createdbsql = "create database " + DBNAME;
            st2.executeQuery(createdbsql);

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