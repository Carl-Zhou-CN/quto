package cn.itcast.map;

import cn.itcast.bean.IndexBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.Timestamp;

public class IndexPutHdfsMap implements MapFunction<IndexBean,String> {
    //分隔符
    private String sp = QuotConfig.config.getProperty("hdfs.seperator");
    @Override
    public String map(IndexBean indexBean) throws Exception {
        /**
         * 开发步骤:
         * 1.定义字符串字段分隔符
         * 2.日期转换和截取：date类型
         * 3.新建字符串缓存对象
         * 4.封装字符串数据
         *
         * 字符串拼装字段顺序：
         * Timestamp|date|indexCode|indexName|preClosePrice|openPirce|highPrice|
         * lowPrice|closePrice|tradeVol|tradeAmt|tradeVolDay|tradeAmtDay|source
         */
        String tradeTime = DateUtil.longTimeToString(indexBean.getEventTime(), Constant.format_yyyy_mm_dd);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(new Timestamp(indexBean.getEventTime())).append(sp)
                .append(tradeTime).append(sp)
                .append(indexBean.getIndexCode()).append(sp)
                .append(indexBean.getIndexName()).append(sp)
                .append(indexBean.getPreClosePrice()).append(sp)
                .append(indexBean.getOpenPrice()).append(sp)
                .append(indexBean.getHighPrice()).append(sp)
                .append(indexBean.getLowPrice()).append(sp)
                .append(indexBean.getClosePrice()).append(sp)
                .append(indexBean.getTradeVol()).append(sp)
                .append(indexBean.getTradeAmt()).append(sp)
                .append(indexBean.getTradeVolDay()).append(sp)
                .append(indexBean.getTradeAmtDay()).append(sp)
                .append(indexBean.getSource());

        return stringBuilder.toString();
    }
}
