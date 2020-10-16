package cn.itcast.map;

import cn.itcast.avro.SseAvro;
import cn.itcast.avro.SzseAvro;
import cn.itcast.bean.CleanBean;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.math.BigDecimal;

public class TMap<T extends SpecificRecordBase> extends RichMapFunction<T, CleanBean> {

    @Override
    public CleanBean map(T tValue) throws Exception {
        CleanBean cleanBean =null;
    if (tValue instanceof SseAvro){
        SseAvro value= (SseAvro) tValue;
        cleanBean= new CleanBean();
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

    }else if (tValue instanceof SzseAvro){
        SzseAvro value= (SzseAvro) tValue;
        cleanBean= new CleanBean();
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
        cleanBean.setSource("szse");
    }
        return cleanBean;
    }

    private  <E>  CleanBean getCleanBean(E tValue){
        SseAvro value= (SseAvro) tValue;
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
        return cleanBean;
    }

}
