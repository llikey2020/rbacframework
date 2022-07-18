package com.sequoiadp.rbac.ddl.all;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : Non super user/non owner GRANT ALL ON VIEW to user
 * @Author        : Lena
 */

public class GrantAllOnDBNoAdminGroup_296B extends SDPTestBase {
    public GrantAllOnDBNoAdminGroup_296B() {
        super.setTableName("tablea");
        super.hasGroup();

    }
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*not have grant privilege on.*")
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null, conn3 = null;
        Statement st1 = null,st2 = null, st3 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", getConfig("testUser"));
            st1.executeQuery(addgpusersql);
            
            String grantusagesql = HiveConnection.getInstance().grantSql("usage","database",getConfig("dbName"),"user",getConfig("nonowner"));
            st1.executeQuery(grantusagesql);
            
            conn3 = HiveConnection.getInstance().getNonownerConnect();
            st3= conn3.createStatement();
            st3.executeQuery(usagesql);
                  
            String grantsqlall = HiveConnection.getInstance().grantSql("all","database",getConfig("dbName"),"group",getConfig("testGroup"));
            st3.executeQuery(grantsqlall);
            
            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();

            st2.executeQuery(usagesql);
            

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            if(st2 != null)st2.close();
            st3.close();
            conn1.close();
            if(conn2 != null)conn2.close();
            conn3.close();
        }
    }
}

