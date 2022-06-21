package com.sequoiadp.testcommon;/*
 * @Description   :
 * @Author        :  Fangjun
 * @CreateTime    : 2022/6/21 17:01
 * @LastEditTime  : 2022/6/21 17:01
 * @LastEditors   :
 */

import java.util.Properties;

public class ParaBeen {
    private static Properties conf=new Properties();

    public static void setConfig(String key,String value){
        conf.setProperty(key, value);
    }

    public static String getConfig(String key){
        return conf.getProperty(key);
    }
}
