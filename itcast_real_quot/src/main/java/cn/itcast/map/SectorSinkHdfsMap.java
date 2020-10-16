package cn.itcast.map;

import cn.itcast.bean.SectorBean;
import cn.itcast.config.QuotConfig;
import cn.itcast.constant.Constant;
import cn.itcast.util.DateUtil;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.Timestamp;
import java.util.Date;

public class SectorSinkHdfsMap implements MapFunction<SectorBean, String> {
    private String sp = QuotConfig.config.getProperty("hdfs.seperator");
    @Override
    public String map(SectorBean sectorBean) throws Exception {
        String tradeDate = DateUtil.longTimeToString(sectorBean.getEventTime(), Constant.format_yyyy_mm_dd);
        StringBuilder builder = new StringBuilder();
        builder.append(new Timestamp(new Date().getTime())).append(sp)
                .append(tradeDate).append(sp)
                .append(sectorBean.getSectorCode()).append(sp)
                .append(sectorBean.getSectorName()).append(sp)
                .append(sectorBean.getPreClosePrice()).append(sp)
                .append(sectorBean.getOpenPrice()).append(sp)
                .append(sectorBean.getHighPrice()).append(sp)
                .append(sectorBean.getLowPrice()).append(sp)
                .append(sectorBean.getClosePrice()).append(sp)
                .append(sectorBean.getTradeVol()).append(sp)
                .append(sectorBean.getTradeAmt()).append(sp)
                .append(sectorBean.getTradeVolDay()).append(sp)
                .append(sectorBean.getTradeAmtDay());
        System.out.println(builder.toString());
        return builder.toString();
    }
}
