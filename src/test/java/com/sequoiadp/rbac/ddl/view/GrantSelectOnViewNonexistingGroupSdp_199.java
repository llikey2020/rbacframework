package com.sequoiadp.rbac.ddl.view;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPViewTestBase;
import org.testng.annotations.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT SELECT ON VIEW TO GROUP <non existing group name>
 * @Author        : Lena
 *
 */

public class GrantSelectOnViewNonexistingGroupSdp_199 extends SDPViewTestBase {
    public GrantSelectOnViewNonexistingGroupSdp_199() {
        super.setTableName("tablea");
        super.setViewName(this.getTableName() + "_view");
        super.hasGroup();
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
            String grantsqlview = HiveConnection.getInstance().grantSql("select","view",viewName,"group","nonexistinggroup");
            st1.executeQuery(grantsqlview);
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            conn1.close();
        }
    }
}

