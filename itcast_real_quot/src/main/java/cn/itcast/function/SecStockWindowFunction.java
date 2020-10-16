package cn.itcast.function;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import cn.itcast.util.FunctionUtil;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.jasper.tagplugins.jstl.core.Out;

public class SecStockWindowFunction implements WindowFunction<CleanBean, StockBean,String, TimeWindow> {

    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<CleanBean> input, Collector<StockBean> collector) throws Exception {
        /**
         * 1.新建SecStockWindowFunction 窗口函数
         * 2.记录最新个股
         * 3.格式化日期
         * 4.封装输出数据
         */
        //2.记录最新个股
        CleanBean cleanBean= FunctionUtil.newCleanBean(input);
        //3格式化日期
        Long tradeTime = DateUtil.longTimeTransfer(cleanBean.getEventTime(), Constant.format_YYYYMMDDHHMMSS);
        //eventTime、secCode、secName、preClosePrice、openPrice、highPrice、lowPrice、closePrice、
        //tradeVol、tradeAmt、tradeVolDay、tradeAmtDay、tradeTime、source
        //4.封装输出数据
        StockBean stockBean = new StockBean(
                cleanBean.getEventTime(),
                cleanBean.getSecCode(),
                cleanBean.getSecName(),
                cleanBean.getPreClosePx(),
                cleanBean.getOpenPrice(),
                cleanBean.getMaxPrice(),
                cleanBean.getMinPrice(),
                cleanBean.getTradePrice(),
                cleanBean.getTradeVolumn(),
                cleanBean.getTradeAmt(),
                0L, 0L,
                tradeTime,
                cleanBean.getSource()
        );

        collector.collect(stockBean);

    }
}
