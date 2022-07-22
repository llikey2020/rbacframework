package com.sequoiadp.rbac.ddl.other;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
/*
 * @Description   : drop user from group which has privilege assigned
 * @Author        : Lena
 */

public class DropUserFromGroupHasPrivilege_Sdp_322 extends SDPTestBase {
	public DropUserFromGroupHasPrivilege_Sdp_322() {
		super.setTableName("tablea");
		super.hasGroup();
	}

	 @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".* does not have select privilege on .*")
	public void test() throws SQLException {
		Connection conn1 = null, conn2 = null;
		Statement st1 = null, st2 = null;
		try {
			// 管理员连接到thriftserver
			conn1 = HiveConnection.getInstance().getAdminConnect();
			st1 = conn1.createStatement();

			String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
			st1.executeQuery(usagesql);

			String grantsql = HiveConnection.getInstance().grantSql("select", "table", tableName, "group",
					getConfig("testGroup"));
			st1.executeQuery(grantsql);
			
			String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"), "add",
					getConfig("testUser"));
			st1.executeQuery(addgpusersql);
			// 测试用户连接到thriftserver
			conn2 = HiveConnection.getInstance().getTestConnect();
			st2 = conn2.createStatement();

			String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"), tableName);
			st2.executeQuery(selectsql);
			
			String dropgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"), "drop",
					getConfig("testUser"));
			st1.executeQuery(dropgpusersql);
			
			st2.executeQuery(selectsql);			

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
