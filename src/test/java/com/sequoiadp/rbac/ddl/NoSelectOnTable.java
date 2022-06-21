package com.sequoiadp.rbac.ddl;/*
 * @Description   : sequoiadb not grant select on table to user testuser
 * @Author        :  Fangjun
 * @CreateTime    : 2022/6/6 16:17
 * @LastEditTime  : 2022/6/6 16:17
 * @LastEditors   :
 */

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class NoSelectOnTable extends SDPTestBase {
    public NoSelectOnTable() {
        super.setTableName("njuty0j7");
    }
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".* does not have select privilege on .*")
    public void test() throws SQLException {
        Connection conn2= null;
        Statement st2 =null;
        try {

            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();

            String selectsql =HiveConnection.getInstance().selectTv(getConfig("dbName"),tableName);
            st2.executeQuery(selectsql);

        } finally {
            st2.close();
            conn2.close();
        }
    }
}
