package com.sequoiadp.testcommon;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class HiveConnection {
    private static HiveConnection instsance;
    private static Lock lock= new ReentrantLock();
    public static HiveConnection getInstance(){
        if(instsance==null){
            instsance=new HiveConnection();
        }
        return instsance;
    }

    //通过驱动连接数据库
    public Connection getAdminConnect () {
        return getConnect(ParaBeen.getConfig("rootUser"),ParaBeen.getConfig("rootPwd"));
    }

    public Connection getTestConnect (){
        return getConnect(ParaBeen.getConfig("testUser"),ParaBeen.getConfig("testPwd"));
    }

    public  Connection getConnect (String user,String pwd) {
        synchronized(lock) {
            Connection conn = null;
            try {
                Class.forName("org.apache.hive.jdbc.HiveDriver");
                conn = DriverManager.getConnection(ParaBeen.getConfig("url"), user, pwd);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            return conn;
        }
    }
    public String grantSql (String privilege,String dbType,String Dtname,String userType,String username){
        StringBuilder sb=new StringBuilder();
        sb.append("grant ").append(privilege).append(" on ").append(dbType).append(' ');
        sb.append(Dtname).append(" to ").append(userType).append(' ').append(username).append(';');
        return sb.toString();
    }

    public String revokeSql (String privilege,String dbType,String Dtname,String userType,String username){
        StringBuilder sb=new StringBuilder();
        sb.append("revoke ").append(privilege).append(" on ").append(dbType).append(' ');
        sb.append(Dtname).append(" from ").append(userType).append(' ').append(username).append(';');
        return sb.toString();
    }


    public String selectTv (String dbName,String tabname){
        String s = "select * from " + dbName + "." + tabname + ";";
        return s;
    }
    public String usageSql(String dbName){
        return "use " + dbName + ";" ;
    }

    public String alterUserSql(String groupName,String action,String testUser){
        return "alter group " + groupName + " " + action + " user " + testUser + ";";
    }

    public String dropSql(String dt,String name){
        return "drop "+ dt + " " + name + ";";
    }
}
