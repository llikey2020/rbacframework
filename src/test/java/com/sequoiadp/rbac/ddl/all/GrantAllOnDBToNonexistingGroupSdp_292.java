package com.sequoiadp.rbac.ddl.all;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT ALL ON DATABASE TO GROUP <non existing groupname>
 * @Author        : Lena
 */

public class GrantAllOnDBToNonexistingGroupSdp_292 extends SDPTestBase {

    public static final String TABLENAME = "newtablename";
    public GrantAllOnDBToNonexistingGroupSdp_292() {
        super.setTableName("tablea");
    }
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*Operation not allowed.*")
    public void test() throws SQLException {
    	
        Connection conn1 = null;
        Statement st1 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);

            String grantsql = HiveConnection.getInstance().grantSql("all","database",getConfig("dbName"),"user","nonexistinggroup");
            st1.executeQuery(grantsql);
            
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", "nonexistinggroup");
            st1.executeQuery(addgpusersql);

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
//            st2.close();
            conn1.close();
//            conn2.close();
        }
    }
}
