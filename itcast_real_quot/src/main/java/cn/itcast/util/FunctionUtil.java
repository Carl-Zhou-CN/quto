package cn.itcast.util;

import cn.itcast.bean.CleanBean;

public class FunctionUtil {

    public static CleanBean newCleanBean(Iterable<CleanBean> input){
        CleanBean cleanBean=null;
        for (CleanBean line : input){
            if ( null == cleanBean){
                cleanBean=line;
            }
            if (line.getEventTime() > cleanBean.getEventTime()){
                cleanBean=line;
            }
        }
        return cleanBean;
    }
}
