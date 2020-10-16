package cn.itcast.map;

import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.math.BigDecimal;

public class SzseMap extends RichMapFunction<SzseAvro, CleanBean> {
    @Override
    public CleanBean map(SzseAvro value) throws Exception {
        CleanBean cleanBean = new CleanBean();
        cleanBean.setMdStreamId(String.valueOf(value.getMdStreamID()));
        cleanBean.setSecCode(String.valueOf(value.getSecurityID()));
        cleanBean.setSecName(String.valueOf(value.getSymbol()));
        cleanBean.setTradeVolumn(value.getTradeVolume());
        cleanBean.setTradeAmt(value.getTotalValueTraded());
        cleanBean.setPreClosePx(BigDecimal.valueOf(value.getPreClosePx()));
        cleanBean.setOpenPrice(BigDecimal.valueOf(value.getOpenPrice()));
        cleanBean.setMaxPrice(BigDecimal.valueOf(value.getHighPrice()));
        cleanBean.setMinPrice(BigDecimal.valueOf(value.getLowPrice()));
        cleanBean.setTradePrice(BigDecimal.valueOf(value.getTradePrice()));
        cleanBean.setEventTime(value.getTimestamp());
        cleanBean.setSource("sse");
        return cleanBean;
    }
}
