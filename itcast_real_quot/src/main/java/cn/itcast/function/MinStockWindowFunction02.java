package cn.itcast.function;

import cn.itcast.bean.CleanBean;
import cn.itcast.bean.StockBean;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import cn.itcast.util.FunctionUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


public class MinStockWindowFunction02 extends RichWindowFunction<CleanBean, StockBean,String, TimeWindow> {
    MapState<String, StockBean> mapState;
    @Override
    public void open(Configuration parameters) throws Exception {
      mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, StockBean>("mapState", String.class, StockBean.class));
    }

    @Override
    public void apply(String s, TimeWindow window, Iterable<CleanBean> input, Collector<StockBean> out) throws Exception {
        //求分时成交量和分时成交金额,封装StockBean对象
        CleanBean cleanBean = FunctionUtil.newCleanBean(input);
        StockBean stockBeanMap = mapState.get(cleanBean.getSecCode());
        long minTradeVol = 0L;
        long minTradeAmt = 0L;
        if (null != stockBeanMap){
            Long tradeAmtDay = stockBeanMap.getTradeAmtDay();
            Long tradeVolDay = stockBeanMap.getTradeVolDay();
            minTradeAmt= tradeAmtDay - cleanBean.getTradeAmt();
            minTradeVol= tradeVolDay - cleanBean.getTradeVolumn();
        }
        //格式化时间
        Long tradeTime = DateUtil.longTimeTransfer(cleanBean.getEventTime(), Constant.format_YYYYMMDDHHMMSS);
        StockBean stockBean = new StockBean(
                cleanBean.getEventTime(),
                cleanBean.getSecCode(),
                cleanBean.getSecName(),
                cleanBean.getPreClosePx(),
                cleanBean.getOpenPrice(),
                cleanBean.getMaxPrice(),
                cleanBean.getMinPrice(),
                cleanBean.getTradePrice(),
                minTradeVol, minTradeAmt,
                cleanBean.getTradeVolumn(),
                cleanBean.getTradeAmt(),
                tradeTime,
                cleanBean.getSource()
        );
        out.collect(stockBean);
        mapState.put(stockBean.getSecCode(), stockBean);
    }
}
