package cn.itcast.function;

import cn.itcast.bean.SectorBean;
import cn.itcast.bean.StockBean;
import cn.itcast.util.DbUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SectorWindowFunction02 extends RichAllWindowFunction<StockBean, SectorBean, TimeWindow> {

    Map<String, List<Map<String, Object>>> sectorStockMap = null;
    MapState<String, SectorBean> sectorLastMap = null;
    BigDecimal basePrice = null;
    Map<String, Map<String, Object>>  sectorDayKlineMap = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        /**
         *   1. 初始化数据：板块对应关系、最近交易日日K
         *   2. 定义状态MapState<String, SectorBean >:上一个窗口板块数据
         *   3. 初始化基准价
         */
        //获取板块下的所有个股,封装在map集合中
        String sql = "SELECT * FROM bdp_sector_stock WHERE sec_abbr = 'ss'";
         sectorStockMap = DbUtil.queryForGroup("sector_code", "sql");
        // 定义状态MapState<String, SectorBean >:上一个窗口板块数据
         sectorLastMap = getRuntimeContext().getMapState(new MapStateDescriptor<String, SectorBean>("sectorLastMap", String.class, SectorBean.class));
         basePrice = new BigDecimal(1000L);
        //获取最近交易日日K
        String sql2 = "SELECT * FROM bdp_quot_sector_kline_day WHERE trade_date = (SELECT last_trade_date FROM tcc_date WHERE trade_date = CURDATE()";
        sectorDayKlineMap = DbUtil.query("sector_code", sql2);
    }

    @Override
    public void apply(TimeWindow window, Iterable<StockBean> values, Collector<SectorBean> out) throws Exception {

        //1.循环窗口内个股数据并缓存
        HashMap<String, StockBean> stockCacheMap  = new HashMap<>();
        for (StockBean value : values) {
            stockCacheMap.put(value.getSecCode(), value);
        }
        // 2.轮询板块对应关系表下的个股数据、并获取指定板块下的个股
        for (String sectorCode : sectorStockMap.keySet()) {
            //板块下所有个股
            List<Map<String, Object>> strockList = sectorStockMap.get(sectorCode);
            //3.初始化全部数据
            Long eventTime = 0l;
            String sectorName = null;
            BigDecimal preClosePrice = new BigDecimal(0);
            BigDecimal openPrice = new BigDecimal(0);
            BigDecimal highPrice = new BigDecimal(0);
            BigDecimal lowPrice = new BigDecimal(0);
            BigDecimal closePrice = new BigDecimal(0);
            Long tradeVol = 0l; //分时成交量
            Long tradeAmt = 0l; //分时成交额
            Long tradeVolDay = 0l; //日成交总量
            Long tradeAmtDay = 0l; //日成交总额
            Long tradeTime = 0l;
            BigDecimal sectorOpenPrie = new BigDecimal(0);//板块开盘价格 
            BigDecimal sectorClosePrie = new BigDecimal(0);//板块收盘价格 
            //4.轮询板块个股数据，并获取板块名称、个股代码、个股流通股本和前一日板块总流通市值

            for (Map<String, Object> stock : strockList) {
                sectorName = stock.get("sector_name").toString();
                String secCode = stock.get("sec_code").toString();
                String negoCap = stock.get("nego_cap").toString();
                String preSectorNegoCap = stock.get("pre_sector_nego_cap").toString();//前一日板块总流通市值
                // 5.计算：开盘价/收盘价累计流通市值、累计（交易量、交易金额）（注意：是个股累计值，获取缓存个股数据）
                //  累计个股开盘流通市值 = SUM(板块下的个股开盘价*个股流通股本)
                //  累计个股收盘流通市值 = SUM(板块下的个股收盘价*个股流通股本)
                BigDecimal openTotalCap = new BigDecimal(0);
                BigDecimal closeTotalCap = new BigDecimal(0);
                StockBean stockBean = stockCacheMap.get(secCode);
                if (null != stockBean){
                   //累计成交额,累计成交总量
                    tradeVolDay = tradeVolDay + stockBean.getTradeVolDay();
                    tradeAmtDay = tradeAmtDay + stockBean.getTradeAmtDay();
                    //开盘价,收盘价
                    BigDecimal openPrice1 = stockBean.getOpenPrice();
                    BigDecimal closePrice1 = stockBean.getClosePrice();
                    //累计个股开盘流通市值 = SUM(板块下的个股开盘价*个股流通股本)
                    openTotalCap = sectorOpenPrie.add(openPrice1.multiply(new BigDecimal(negoCap)));
                    //  累计个股收盘流通市值 = SUM(板块下的个股收盘价*个股流通股本)
                    closeTotalCap = sectorClosePrie.add(closePrice1.multiply(new BigDecimal(negoCap)));
                }
                //判断股票是否为首日上市
                // 板块开盘价格 = 板块前收盘价*当前板块以开盘价计算的总流通市值/当前板块前一交易日板块总流通市值
                // 板块当前价格 = 板块前收盘价*当前板块以收盘价计算的总流通市值/当前板块前一交易日板块总流通市值
                basePrice=new BigDecimal(1000L);
                if (null != sectorDayKlineMap && sectorDayKlineMap.get(sectorCode) == null){
                    sectorOpenPrie= basePrice.multiply(openTotalCap).divide(new BigDecimal(preSectorNegoCap),2, RoundingMode.HALF_UP);
                    sectorClosePrie= basePrice.multiply(closeTotalCap).divide(new BigDecimal(preSectorNegoCap),2, RoundingMode.HALF_UP);
                }else {
                    Map<String, Object> sectorKline  = sectorDayKlineMap.get(sectorCode);
                    if (null != sectorKline && sectorKline.size() > 0){
                        preClosePrice = new BigDecimal(sectorKline.get("close_price").toString());
                        sectorOpenPrie= preClosePrice.multiply(openTotalCap).divide(new BigDecimal(preSectorNegoCap),2, RoundingMode.HALF_UP);
                        sectorClosePrie= preClosePrice.multiply(closeTotalCap).divide(new BigDecimal(preSectorNegoCap),2, RoundingMode.HALF_UP);
                    }
                }
                //   7.初始化板块高低价
                highPrice = sectorClosePrie;
                lowPrice  = sectorClosePrie;
                //8.获取上一个窗口板块数据（高、低、成交量、成交金额）
                SectorBean sectorBeanLast = sectorLastMap.get(sectorCode);
                BigDecimal lastHighPrice = new BigDecimal(0);
                BigDecimal lastLowPrice = new BigDecimal(0);
                long lastTradeAmtDay = 0l;
                long lastTradeVolDay = 0l;
                if (sectorBeanLast != null){
                    lastHighPrice = sectorBeanLast .getHighPrice();
                    lastLowPrice = sectorBeanLast .getLowPrice();
                    lastTradeAmtDay = sectorBeanLast .getTradeAmtDay();
                    lastTradeVolDay = sectorBeanLast .getTradeVolDay();
                }
               //9.计算最高价和最低价（前后窗口比较）
               if (highPrice.compareTo(lastHighPrice) == -1){
                   highPrice = lastHighPrice;
               }
               if (lowPrice.compareTo(lastLowPrice) == 1){
                   lowPrice = lastLowPrice;
               }
               // 10.计算分时成交量和成交金额
                tradeVol = tradeVolDay - lastTradeVolDay;
                tradeVol = tradeAmtDay - lastTradeAmtDay;
                //11.开盘价与高低价比较
                if (highPrice.compareTo(openPrice) == -1){
                    highPrice = openPrice;
                }
                if (lowPrice.compareTo(openPrice) == 1){
                    lowPrice = openPrice;
                }
                // 12.封装结果数据
                SectorBean sectorBean = new SectorBean();
                sectorBean.setEventTime(eventTime);
                sectorBean.setSectorCode(sectorCode);
                sectorBean.setSectorName(sectorName);
                sectorBean.setPreClosePrice(preClosePrice);
                sectorBean.setOpenPrice(openPrice);
                sectorBean.setHighPrice(highPrice);
                sectorBean.setLowPrice(lowPrice);
                sectorBean.setClosePrice(closePrice);
                sectorBean.setTradeVol(tradeVol);
                sectorBean.setTradeAmt(tradeAmt);
                sectorBean.setTradeVolDay(tradeVolDay);
                sectorBean.setTradeAmtDay(tradeAmtDay);
                sectorBean.setTradeTime(tradeTime);
                out.collect(sectorBean);

                // 13.缓存当前板块数据
                sectorLastMap.put(sectorCode,sectorBean);
            }
            
        }
    }
}
