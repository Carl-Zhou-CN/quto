package cn.itcast.map;

import cn.itcast.bean.StockBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.Timestamp;

public class StockPutHdfsMap02 implements MapFunction<StockBean,String> {
    String sp = QuotConfig.config.getProperty("hdfs.seperator");
    @Override
    public String map(StockBean stockBean) throws Exception {
        String tradeDate = DateUtil.longTimeToString(stockBean.getEventTime(), Constant.format_yyyy_mm_dd);
        StringBuilder stringBuilder = new StringBuilder();
        //拼接字符串
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
