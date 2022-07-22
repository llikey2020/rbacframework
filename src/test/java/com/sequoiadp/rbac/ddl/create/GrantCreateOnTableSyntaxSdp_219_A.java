package com.sequoiadp.rbac.ddl.create;
import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT CREATE ON TABLE TO USER syntax validation
 * @Author        : Lena
 */

public class GrantCreateOnTableSyntaxSdp_219_A extends SDPTestBase {
	
    public static final String TABLENAME = "newtablename";
    //测试点
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
            String grantsqlview = HiveConnection.getInstance().grantSql("creates","table",TABLENAME,"user",getConfig("testUser"));
            st1.executeQuery(grantsqlview);
            
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
            if(st2 != null) st2.close();
            conn1.close();
            if(conn2 != null) conn2.close();
        }
    }
}


