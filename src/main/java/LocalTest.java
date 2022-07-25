import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import func.MyFlinkCDCDeSer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigInteger;
import java.sql.Struct;
import java.util.Collection;
import java.util.Set;

public class LocalTest {
    public static void main(String[] args) {
        //Flink流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //本地开启WebUI使用环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        //开启CK
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //开启两条流，一条主流inputDS，一条控制流controlDS
        DataStreamSource<String> inputDS = env.readTextFile("data/kafkaSource.txt");
//        DataStreamSource<String> controlDS = env.readTextFile("data/control.txt");

        DebeziumSourceFunction<String> mySQLSource = MySQLSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("mytest")
                .tableList("mytest.ta_configure")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyFlinkCDCDeSer())
                .build();
        DataStreamSource<String> controlDS = env.addSource(mySQLSource);


        //创建状态描述器，把控制流广播出去
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("boradcast-state", Types.STRING, Types.STRING);
        BroadcastStream<String> contrlBS = controlDS.broadcast(mapStateDescriptor);

        //过滤主流中不是JSON格式的数据
        SingleOutputStreamOperator<String> inputFilterDS = inputDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return func.MyUtil.isJSON(s);
            }
        });

        //连接主流与控制流
        BroadcastConnectedStream<String, String> connectDS = inputFilterDS.connect(contrlBS);

        //调用自定义的BroadcastProcessFunction完成数数格式的转换
        SingleOutputStreamOperator<String> resultDS = connectDS.process(new CastProcessFunction(mapStateDescriptor));


        inputDS.print();
        resultDS.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
