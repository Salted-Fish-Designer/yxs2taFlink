import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import func.CastProcessFunction;
import func.MyFlinkCDCDeSer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.IOException;
import java.util.Properties;


public class TaFormat {
    public static void main(String[] args) {
        //TODO 0.获取配置信息
        if (args.length != 1) {
            System.out.println("args.length must be 1 ,please check it!!");
            System.exit(-1);
        }
        ParameterTool tool = null;
        String propertiesPath = args[0];
        try {
            tool = ParameterTool.fromPropertiesFile(propertiesPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Properties properties = tool.getProperties();
        //获取MySQL配置信息
        String mysql_host = properties.getProperty("mysql.host");
        String mysql_port = properties.getProperty("mysql.port");
        String mysql_databaseList = properties.getProperty("mysql.databaseList");
        String mysql_tableList = properties.getProperty("mysql.tableList");
        String mysql_username = properties.getProperty("mysql.username");
        String mysql_password = properties.getProperty("mysql.password");
        //获取kafka配置信息
        String kafka_brokers = properties.getProperty("kafka.brokers");
        String kafka_topic_source = properties.getProperty("kafka.topic");
        String kafka_group = properties.getProperty("kafka.group");
        String kafka_topic_ta = properties.getProperty("kafka.topic.ta");


        //TODO 1.创建Flink流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //本地开启WebUI使用环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        //开启CK
        env.enableCheckpointing(60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        //TODO 2.开启两条流，一条主流inputDS，一条控制流controlDS
        //从kafka获取数据主流
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafka_brokers)
                .setTopics(kafka_topic_source)
                .setGroupId(kafka_group)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> inputDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        //过滤主流中不是JSON格式的数据
        SingleOutputStreamOperator<String> inputFilterDS = inputDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return func.MyUtil.isJSON(s);
            }
        });

        //通过FlinkCDC读取MySQL，创建控制流controlDS
        DebeziumSourceFunction<String> mySQLSource = MySQLSource.<String>builder()
                .hostname(mysql_host)
                .port(Integer.parseInt(mysql_port))
                .username(mysql_username)
                .password(mysql_password)
                .databaseList(mysql_databaseList)
                .tableList(mysql_tableList)
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyFlinkCDCDeSer())
                .build();
        DataStreamSource<String> controlDS = env.addSource(mySQLSource);

        //TODO 3.创建状态描述器，把控制流广播出去
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("boradcast-state", Types.STRING, Types.STRING);
        BroadcastStream<String> contrlBS = controlDS.broadcast(mapStateDescriptor);

        //TODO 4.连接主流与广播控制流
        BroadcastConnectedStream<String, String> connectDS = inputFilterDS.connect(contrlBS);

        //TODO 5.调用自定义的BroadcastProcessFunction完成数数格式的转换
        SingleOutputStreamOperator<String> resultDS = connectDS.process(new CastProcessFunction(mapStateDescriptor));


        //TODO 6.将处理后的数据发送会kafka，flink1.13.6 kafka source与sink的写法未统一
        FlinkKafkaProducer<String> shushu_test = new FlinkKafkaProducer<>(kafka_brokers, kafka_topic_ta, new SimpleStringSchema());
        resultDS.addSink(shushu_test);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
