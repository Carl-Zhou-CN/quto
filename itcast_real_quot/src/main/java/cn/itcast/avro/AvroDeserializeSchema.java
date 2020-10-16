package cn.itcast.avro;

import cn.itcast.config.QuotConfig;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * 开发步骤：
 * 1.创建泛型反序列化类实现反序列化接口
 * 2.创建构造方法
 * 3.avro反序列化数据
 * 4.获取反序列化数据类型
 */

public class AvroDeserializeSchema<T> implements DeserializationSchema<T> {
    private String topicName;

    public AvroDeserializeSchema(String topicName){

        this.topicName=topicName;
    }

    @Override
    public T deserialize(byte[] bytes) throws IOException {
        SpecificDatumReader<T> AvroSpecificDatumReader;
        if (QuotConfig.config.getProperty("sse.topic").equals(topicName)){
             AvroSpecificDatumReader = new SpecificDatumReader(SseAvro.class);
        }else {
             AvroSpecificDatumReader = new SpecificDatumReader(SzseAvro.class);
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(bis, null);
        return AvroSpecificDatumReader.read(null,binaryDecoder );
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        TypeInformation<T> typeInformation;
        if(QuotConfig.config.getProperty("sse.topic").equals(topicName)){
            typeInformation = (TypeInformation<T>) TypeInformation.of(SseAvro.class);
        }else {
             typeInformation = (TypeInformation<T>) TypeInformation.of(SzseAvro.class);
        }
        return  typeInformation;
    }
}
