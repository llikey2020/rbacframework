package com.sequoiadp.testcommon;


/*
 * @Description   : Test view related functions based on SDPTestBase
 * @Author        : Lena
 * @CreateTime    : 2022/6/27 10:04
 * @LastEditTime  : 2022/6/27 10:04
 * @LastEditors   : 2022/6/27
 */
import org.testng.annotations.*;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class SDPViewTestBase extends SDPTestBase {


    protected String viewName;


    public void setViewName(String viewname){
    	viewName=viewname;
    }

    protected void notUsage(){
        hasUsage=false;
    }

    protected void hasGroup(){
        hasGroup=true;
    }

    @BeforeClass
    @Override
    public void setup() throws SQLException {
        conn = HiveConnection.getInstance().getAdminConnect();
        Statement st ;
        st = conn.createStatement();
        String usesql = "use " + getConfig("dbName") + ";" ;
        st.executeQuery(usesql);
        //建表
        String createtablsql = "create table if not exists " + tableName + "(id int,name varchar(20));";
        st.executeQuery(createtablsql);
        //插入数据
        String insertsql = "insert into " + tableName + " values(1001,'swtshs');";
        st.executeQuery(insertsql);
        if(hasUsage) {
            String grantDatabase = HiveConnection.getInstance().grantSql("usage","database",getConfig("dbName"),"user",getConfig("testUser"));
            st.executeQuery(grantDatabase);
        }
        if(hasGroup){
            String creategroup = "create group "+getConfig("testGroup") + " ;";
            st.executeQuery(creategroup);
        }

        String viewsql = "create view " + viewName + " as select * from " + tableName ;
        st.executeQuery(viewsql);
        st.close();
        
    }

    @AfterClass
    @Override
    public void tearDown() throws SQLException {
        Statement st;
        st = conn.createStatement();
        String usageSql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
        st.executeQuery(usageSql);
        if(hasGroup){
            String dropgroup = "drop group " + getConfig("testGroup") + ";";
            st.executeQuery(dropgroup);
        }
        if(hasUsage) {
            String revokeusagesql  = HiveConnection.getInstance().revokeSql("usage","database",getConfig("dbName"),"user",getConfig("testUser"));
            st.executeQuery(revokeusagesql);
        }
        
        String dropviewsql = HiveConnection.getInstance().dropSql("view",viewName);
        st.executeQuery(dropviewsql);
        
        String droptablesql = HiveConnection.getInstance().dropSql("table",tableName);
        st.executeQuery(droptablesql);

        st.close();
    }

}
