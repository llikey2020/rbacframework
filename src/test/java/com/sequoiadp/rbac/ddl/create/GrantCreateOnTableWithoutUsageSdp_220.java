package com.sequoiadp.rbac.ddl.create;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT CREATE ON TABLE without granting usage to user
 * @Author        : Lena
 */

public class GrantCreateOnTableWithoutUsageSdp_220 extends SDPTestBase {
    public GrantCreateOnTableWithoutUsageSdp_220() {
        super.notUsage();
    }
    
    public static final String TABLENAME = "newtablename";
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*does not have USAGE privilege on.*")
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String grantsql = HiveConnection.getInstance().grantSql("create","table",TABLENAME,"user",getConfig("testUser"));
            st1.executeQuery(grantsql);
                      
            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();
            
            String s3 = "s3a://sdbbucket2/" + TABLENAME;
            //建表
            String createtablesql = "create table " + getConfig("dbName") + "." + TABLENAME + " (id int)using delta location \"" + s3 + "\" " + ";" ;
            st2.executeQuery(createtablesql);
            
            String droptablesql = HiveConnection.getInstance().dropSql("table",getConfig("dbName") + "." + TABLENAME );
            st1.executeQuery(droptablesql);
                      
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

