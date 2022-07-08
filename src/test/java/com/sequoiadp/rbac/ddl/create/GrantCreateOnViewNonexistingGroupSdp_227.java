package com.sequoiadp.rbac.ddl.create;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT CREATE ON VIEW TO GROUP <non existing group name>
 * @Author        : Lena
 */

public class GrantCreateOnViewNonexistingGroupSdp_227 extends SDPTestBase {
	
	public GrantCreateOnViewNonexistingGroupSdp_227() {
		super.setTableName("tablea");
		super.hasGroup();
	}

	public static final String VIEWNAME = "newviewname";
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
            String grantsqlselect = HiveConnection.getInstance().grantSql("select","table",tableName,"user",getConfig("testUser"));
            st1.executeQuery(grantsqlselect);           
			String grantsqlview = HiveConnection.getInstance().grantSql("create", "view", VIEWNAME, "group",
					"nonexistinggroup");
			st1.executeQuery(grantsqlview);
        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            conn1.close();
        }
    }
}

