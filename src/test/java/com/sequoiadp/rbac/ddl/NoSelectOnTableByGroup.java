package com.sequoiadp.rbac.ddl;/*
 * @Description   :
 * @Author        :  Fangjun
 * @CreateTime    : 2022/6/6 16:28
 * @LastEditTime  : 2022/6/6 16:28
 * @LastEditors   :
 */

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class NoSelectOnTableByGroup extends SDPTestBase {
    public NoSelectOnTableByGroup() {
        super.setTableName("sdgggsdg");
        super.hasGroup();
    }
    @Test(expectedExceptions = SQLException.class,expectedExceptionsMessageRegExp = ".* does not have select privilege on .*")
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();

            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add",getConfig("testUser"));
            st1.executeQuery(addgpusersql);

            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();

            String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"),tableName);
            st2.executeQuery(selectsql);

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            st2.close();
            conn1.close();
            conn2.close();
        }
    }
}
