package com.sequoiadp.rbac.ddl.create;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;

/*
 * @Description   : GRANT CREATE ON VIEW TO USER
 * @Author        : Lena
 */
public class GrantCreateOnViewSdp_222 extends SDPTestBase {

	public GrantCreateOnViewSdp_222() {
		super.setTableName("tablea");
	}

	public static final String VIEWNAME = "newviewname";

//测试点
	@Test
	public void test() throws SQLException {
		Connection conn1 = null, conn2 = null;
		Statement st1 = null, st2 = null;
		try {
			// 管理员sequoiadb连接到thriftserver
			conn1 = HiveConnection.getInstance().getAdminConnect();
			st1 = conn1.createStatement();
			String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
			st1.executeQuery(usagesql);

            String grantsql = HiveConnection.getInstance().grantSql("select","table",tableName,"user",getConfig("testUser"));
            st1.executeQuery(grantsql);
            
			String grantsqlview = HiveConnection.getInstance().grantSql("create", "view", VIEWNAME, "user",
					getConfig("testUser"));
			st1.executeQuery(grantsqlview);

			// 测试用户test来验证管理员的语句
			conn2 = HiveConnection.getInstance().getTestConnect();
			st2 = conn2.createStatement();
			
			String testusagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
			st2.executeQuery(testusagesql);
			
			String viewsql = "create view " + VIEWNAME + " as select * from " + tableName;
			st2.executeQuery(viewsql);

			String dropsql = HiveConnection.getInstance().dropSql("view", getConfig("dbName") + "." + VIEWNAME);
			st1.executeQuery(dropsql);

		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			st1.close();
			st2.close();
			conn1.close();
			conn2.close();
		}
	}
}
