package com.sequoiadp.rbac.ddl.read_metadata;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT READ_METADATA ON < non existing db>to GROUP
 * @Author        : Lena
 */

public class GrantRead_metadataOnNonexistingDBGroupSdp_311_B extends SDPTestBase {
    public GrantRead_metadataOnNonexistingDBGroupSdp_311_B() {
        super.setTableName("tablea");
        super.hasGroup();
    }
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*update message Database name cannot be found in table name.*")
    public void test() throws SQLException {
        Connection conn1 = null;
        Statement st1 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", getConfig("testUser"));
            st1.executeQuery(addgpusersql);
            String grantsql = HiveConnection.getInstance().grantSql("read_metadata","database","nonexistingdb","group",getConfig("testGroup"));
            st1.executeQuery(grantsql);
        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            conn1.close();
        }
    }
}
