package cn.itcast.map;

import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.Timestamp;

public class StockPutHdfsMap implements MapFunction<StockBean,String> {
    String sp = QuotConfig.config.getProperty("hdfs.seperator");
    @Override
    public String map(StockBean stockBean) throws Exception {
        /**
         * 开发步骤:
         * 1.定义字符串字段分隔符
         * 2.日期转换和截取：date类型
         * 3.新建字符串缓存对象
         * 4.封装字符串数据
         */
       String tradeDate = DateUtil.longTimeToString(stockBean.getEventTime(), Constant.format_yyyy_mm_dd);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(new Timestamp(stockBean.getEventTime())).append(sp)
                .append(tradeDate).append(sp)
                .append(stockBean.getSecCode()).append(sp)
                .append(stockBean.getSecName()).append(sp)
                .append(stockBean.getPreClosePrice()).append(sp)
                .append(stockBean.getOpenPrice()).append(sp)
                .append(stockBean.getHighPrice()).append(sp)
                .append(stockBean.getLowPrice()).append(sp)
                .append(stockBean.getClosePrice()).append(sp)
                .append(stockBean.getTradeVol()).append(sp)
                .append(stockBean.getTradeAmt()).append(sp)
                .append(stockBean.getTradeVolDay()).append(sp)
                .append(stockBean.getTradeAmtDay()).append(sp)
                .append(stockBean.getSource());

        return stringBuilder.toString();
    }
}
