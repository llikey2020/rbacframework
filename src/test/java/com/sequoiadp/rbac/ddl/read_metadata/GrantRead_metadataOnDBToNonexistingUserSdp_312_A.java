package com.sequoiadp.rbac.ddl.read_metadata;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT READ_METADATA ON DATABASE to <non existing username>
 * @Author        : Lena
 */

public class GrantRead_metadataOnDBToNonexistingUserSdp_312_A extends SDPTestBase {
    public GrantRead_metadataOnDBToNonexistingUserSdp_312_A() {
        super.setTableName("tablea");
    }
    //测试点
    @Test
    public void test() throws SQLException {
        Connection conn1 = null;
        Statement st1 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String grantsql = HiveConnection.getInstance().grantSql("read_metadata","database",getConfig("dbName"),"user","nonexistinguser");
            st1.executeQuery(grantsql);
            //测试用户test来验证管理员的语句


        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            conn1.close();
        }
    }
}
