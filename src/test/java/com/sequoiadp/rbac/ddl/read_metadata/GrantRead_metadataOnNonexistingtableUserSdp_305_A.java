package com.sequoiadp.rbac.ddl.read_metadata;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT READ_METADATA ON < non-existing table >to USER
 * @Author        : Lena
 */

public class GrantRead_metadataOnNonexistingtableUserSdp_305_A extends SDPTestBase {
    public GrantRead_metadataOnNonexistingtableUserSdp_305_A() {
        super.setTableName("tablea");
    }
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*update message once issue is fixed.*")
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);

            String grantsql = HiveConnection.getInstance().grantSql("read_metadata","table","noexistingtable","user",getConfig("testUser"));
            st1.executeQuery(grantsql);
          

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


