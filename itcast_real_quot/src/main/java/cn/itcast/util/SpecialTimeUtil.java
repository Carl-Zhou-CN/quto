package cn.itcast.util;

import cn.itcast.config.QuotConfig;
import cn.itcast.constant.Constant;

import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

public class SpecialTimeUtil {

    static long openTime=0;
    static long closeTime=0;

    static {
        //获取配置文件
        Properties properties  = QuotConfig.config;
        String date = properties.getProperty("date");
        String beginTime  = properties.getProperty("open.time");
        String endTime = properties.getProperty("close.time");
        System.out.println(endTime);
        //新建Calendar对象,设置日期
        Calendar calendar = Calendar.getInstance();

        if (date == null){
            calendar.setTime(new Date());
        }else {
            calendar.setTimeInMillis(DateUtil.stringToLong(date, Constant.format_yyyymmdd));
        }
        //设置开市时间
        if (beginTime == null){
            calendar.set(Calendar.HOUR_OF_DAY, 9);
            calendar.set(Calendar.MINUTE, 30);
        }else {
            String[] arr = beginTime.split(":");
            calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(arr[0]));
            calendar.set(Calendar.MINUTE, Integer.parseInt(arr[1]));
        }

        //设置秒
        calendar.set(Calendar.SECOND, 0);

        openTime= calendar.getTime().getTime();
        System.out.println(openTime);
        //设置闭市时间
        if (endTime == null){
            calendar.set(Calendar.HOUR_OF_DAY, 15);
            calendar.set(Calendar.MINUTE, 0);
        }else {
            String[] arr = endTime.split(":");
            calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(arr[0]));
            calendar.set(Calendar.MINUTE, Integer.parseInt(arr[1]));
        }

        //获取闭市时间
        closeTime=calendar.getTime().getTime();
        System.out.println(closeTime);
    }
}
