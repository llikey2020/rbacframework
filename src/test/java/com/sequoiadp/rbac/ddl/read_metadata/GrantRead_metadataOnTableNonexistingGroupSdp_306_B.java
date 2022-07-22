
package com.sequoiadp.rbac.ddl.read_metadata;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

/*
 * @Description   : GRANT READ_METADATA ON TABLE to <non existing group>
 * @Author        : Lena
 */

public class GrantRead_metadataOnTableNonexistingGroupSdp_306_B extends SDPTestBase {
    public GrantRead_metadataOnTableNonexistingGroupSdp_306_B() {
        super.setTableName("tablea");
        super.hasGroup();
    }
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*update message once issue is fixed.*")
    public void test() throws SQLException {
        Connection conn1 = null;
        Statement st1 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String grantsql = HiveConnection.getInstance().grantSql("read_metadata","table",tableName,"group","nonexistinggroup");
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

