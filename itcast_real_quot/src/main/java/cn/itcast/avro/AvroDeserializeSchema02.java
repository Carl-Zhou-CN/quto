package cn.itcast.avro;

import cn.itcast.config.QuotConfig;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class AvroDeserializeSchema02<T> implements DeserializationSchema<T> {
    private String tableName;
    public AvroDeserializeSchema02(String tableName) {
        this.tableName=tableName;
    }
    //反序列方法
    @Override
    public T deserialize(byte[] bytes) throws IOException {
        SpecificDatumReader<T> tSpecificDatumReader;
        if (QuotConfig.config.getProperty("sse.topic").equals(tableName)){
            tSpecificDatumReader=new SpecificDatumReader(SseAvro.class);
        }else {
            tSpecificDatumReader=new SpecificDatumReader(SzseAvro.class);
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        BinaryDecoder binaryDecoder = new DecoderFactory().directBinaryDecoder(bis, null);
        return tSpecificDatumReader.read(null, binaryDecoder);
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }
    //序列化方法
    @Override
    public TypeInformation<T> getProducedType() {
        TypeInformation<T> typeInformation;
        if (QuotConfig.config.getProperty("sse.topic").equals(tableName)){
            typeInformation = (TypeInformation<T>) TypeInformation.of(SseAvro.class);
        }else {
             typeInformation = (TypeInformation<T>) TypeInformation.of(SzseAvro.class);
        }
        return typeInformation;
    }
}
