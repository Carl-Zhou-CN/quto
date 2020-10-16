package cn.itcast.demo;

import cn.itcast.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class AvroDemo {
    public static void main(String[] args) throws IOException {
        //1.新建对象
        User user1 = new User();
        //2.设置数据
        user1.setName("小明");
        user1.setAddress("上海市");
        user1.setAge(20);
        /**
         * 构造方法添加参数
         */
        User user2 = new User("小红", 7, "北京");

        User user3 = User.newBuilder()
                .setName("小李")
                .setAge(12)
                .setAddress("上海")
                .build();

    /*    SpecificDatumWriter<User> userSpecificDatumWriter = new SpecificDatumWriter<>(User.class);
        DataFileWriter<User> userDataFileWriter = new DataFileWriter<>(userSpecificDatumWriter);

        DataFileWriter<User> userDataFileWriter1 = userDataFileWriter.create(user1.getSchema(), new File("data/user.avro"));

        userDataFileWriter1.append(user1);
        userDataFileWriter1.append(user2);
        userDataFileWriter1.append(user3);

        userDataFileWriter.close();*/

        SpecificDatumReader<User> userSpecificDatumReader = new SpecificDatumReader<>(User.class);
        DataFileReader<User> users = new DataFileReader<>(new File("data/user.avro"), userSpecificDatumReader);
        User user4=null;
        while (users.hasNext()){
            user4=users.next();
            System.out.println(user4);
        }
    }
}
