/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ideal.sylph.plugins.kafka.flink;

import com.github.harbby.gadtry.base.ArrayType;
import com.github.harbby.gadtry.base.JavaTypes;
import com.github.harbby.gadtry.base.MapType;
import ideal.sylph.etl.Field;
import ideal.sylph.etl.Schema;
import ideal.sylph.etl.SourceContext;
import ideal.sylph.plugins.kafka.flink.canal.CanalRowDeserializationSchema;
import ideal.sylph.plugins.kafka.flink.canal.CanalRowsMessage;
import ideal.sylph.plugins.kafka.flink.tibinlog.TiBinlogDeserializationSchema;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.type.TypeFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.lang.reflect.Type;
import java.util.*;

import static ideal.sylph.runner.flink.engines.StreamSqlUtil.schemaToRowTypeInfo;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public abstract class KafkaBaseSource
{
    private static final long serialVersionUID = 2L;
    private static final String[] KAFKA_COLUMNS = new String[] {"_topic", "_key", "_message", "_partition", "_offset"};

    public abstract FlinkKafkaConsumerBase<Row> getKafkaConsumerBase(List<String> topicSets,
            KafkaDeserializationSchema<Row> deserializationSchema, Properties properties);

    /**
     * 初始化(driver阶段执行)
     **/
    public DataStream<Row> createSource(StreamExecutionEnvironment execEnv, KafkaSourceConfig config, SourceContext context)
    {
        requireNonNull(execEnv, "execEnv is null");
        requireNonNull(config, "config is null");
        /**
         * 多行类型 处理
         */
        if ("multi".equals(config.getValueType())) {
            return createMultiSource(execEnv, config, context);
        } else if ("tidb_binlog".equals(config.getValueType())) {
            return createTiDbBinlogSource(execEnv, config, context);
        }

        String topics = config.getTopics();
        Properties properties = PropertyUtil.getKafkaProperty(config);

        KafkaDeserializationSchema<Row> deserializationSchema = "json".equals(config.getValueType()) ?
                new JsonDeserializationSchema(context.getSchema()) : new RowDeserializer();

        List<String> topicSets = Arrays.asList(topics.split(","));
        //org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
        FlinkKafkaConsumerBase<Row> base = getKafkaConsumerBase(topicSets, deserializationSchema, properties);
        return execEnv.addSource(base);
    }




    /**
     * 初始化(driver阶段执行)
     * 创建多行source
     **/
    public DataStream<Row> createMultiSource(StreamExecutionEnvironment execEnv, KafkaSourceConfig config, SourceContext context) {
        requireNonNull(execEnv, "execEnv is null");
        String[] files = context.getSchema().getFieldNames()
//                .stream()
//                .filter(name->!name.equalsIgnoreCase("_offset") && !name.equalsIgnoreCase("_partition"))
//                .collect(Collectors.toList())
                .toArray(new String[0]);
        String topics = config.getTopics();
        Properties properties = PropertyUtil.getKafkaProperty(config);
        /**
         * kafka topic
         */
        List<String> topicSets = Arrays.asList(topics.split(","));
        /**
         * filtered field list
         */
        KeyedDeserializationSchema<List<CanalRowsMessage>> deserializationSchema =
                new CanalRowDeserializationSchema(config.getFilterTableName(), files, config.getFilterDDL(), context.getSchema());

        FlinkKafkaConsumer<List<CanalRowsMessage>> consumer = new FlinkKafkaConsumer<>(
                topicSets,
                deserializationSchema,
                properties);

        setConsumerStartOffset(config, consumer, topicSets);

        DataStreamSource<List<CanalRowsMessage>> listDataStreamSource = execEnv.addSource(consumer);
        /**
         * 打散
         */
        SingleOutputStreamOperator<CanalRowsMessage> allData = listDataStreamSource
                .flatMap(
                        new FlatMapFunction<List<CanalRowsMessage>, CanalRowsMessage>() {
                            @Override
                            public void flatMap(List<CanalRowsMessage> value,
                                                Collector<CanalRowsMessage> out) throws Exception {
                                value.forEach(f -> {
//                                    System.out.println("==========================="+f.toString()+"==============");
                                    out.collect(f);
                                });
                            }
                        })
                .filter(new FilterFunction<CanalRowsMessage>() {
                    @Override
                    public boolean filter(CanalRowsMessage flatMessage) throws Exception {
                        return !flatMessage.getDdl();
                    }
                });
        /**
         * 转化成Row 返回
         */
        return allData.map(new MapFunction<CanalRowsMessage, Row>() {
            @Override
            public Row map(CanalRowsMessage value) throws Exception {
                return value.getData();
            }
        }).returns(schemaToRowTypeInfo(context.getSchema()));
    }


    /**
     * 初始化(driver阶段执行)
     * 创建 TiDbBinlog source
     **/
    public DataStream<Row> createTiDbBinlogSource(StreamExecutionEnvironment execEnv, KafkaSourceConfig config, SourceContext context) {
        requireNonNull(execEnv, "execEnv is null");
        String topics = config.getTopics();
        Properties properties = PropertyUtil.getKafkaProperty(config);
        /**
         * kafka topic
         */
        List<String> topicSets = Arrays.asList(topics.split(","));
        TiBinlogDeserializationSchema deserializationSchema = new TiBinlogDeserializationSchema(config.getFilterTableName()
                , context.getSchema());
        FlinkKafkaConsumer<List<Row>> consumer = new FlinkKafkaConsumer<>(
                topicSets,
                deserializationSchema,
                properties);
        setConsumerStartOffset(config, consumer, topicSets);
        DataStreamSource<List<Row>> listDataStreamSource = execEnv.addSource(consumer);

        SingleOutputStreamOperator<Row> objectSingleOutputStreamOperator = listDataStreamSource
                .filter(f -> !f.isEmpty())
                .flatMap(
                        new FlatMapFunction<List<Row>, Row>() {
                            @Override
                            public void flatMap(List<Row> value,
                                                Collector<Row> out) throws Exception {
                                value.forEach(f -> {
                                    out.collect(f);
                                });
                            }
                        });

        return objectSingleOutputStreamOperator.returns(schemaToRowTypeInfo(context.getSchema()));
    }

    /**
     * 设置消费者的消费起始位点
     *
     * @param config
     * @param consumer
     * @param topicSets
     */
    private void setConsumerStartOffset(KafkaSourceConfig config, FlinkKafkaConsumer consumer, List<String> topicSets) {
        //设置消费位点
        if (config.getOffsetMode().equalsIgnoreCase("timestamp")) {
            //时间戳
            consumer.setStartFromTimestamp(DateUtils.parse(config.getTimestamp()).getTime());
        } else if (config.getOffsetMode().equalsIgnoreCase("offset")) {
            //offset和partition位点
            String partitions = config.getPartitions();
            Map<KafkaTopicPartition, Long> partitionMaps = new HashMap<>();
            if (StringUtils.isBlank(partitions)) {
                partitions = topicSets.get(0) + ":0:0";
            }
            KafkaTopicPartition partition;
            String[] ps = partitions.split(";");
            for (String p : ps) {
                String[] split = p.split(":");
                partition = new KafkaTopicPartition(split[0], Integer.valueOf(split[1]));
                partitionMaps.put(partition, Long.valueOf(split[2]));
            }
            consumer.setStartFromSpecificOffsets(partitionMaps);
        } else if (config.getOffsetMode().equalsIgnoreCase("group")) {
            consumer.setStartFromGroupOffsets();
            //组最新的位点
        } else if (config.getOffsetMode().equalsIgnoreCase("earliest")) {
            consumer.setStartFromEarliest();
        } else {
            consumer.setStartFromLatest();
        }
    }


    /**
     * 配置信息类
     */
    private static class PropertyUtil {

        public static Properties getKafkaProperty(KafkaSourceConfig config) {
            requireNonNull(config, "config is null");
            String topics = config.getTopics();
            String groupId = config.getGroupid(); //消费者的名字
            String offsetMode = config.getOffsetMode(); //latest earliest

            Properties properties = new Properties();
            for (Map.Entry<String, Object> entry : config.getOtherConfig().entrySet()) {
                if (entry.getValue() != null) {
                    properties.setProperty(entry.getKey(), entry.getValue().toString());
                }
            }

            properties.put("bootstrap.servers", config.getBrokers());  //需要把集群的host 配置到程序所在机器
            //"enable.auto.commit" -> (false: java.lang.Boolean), //不自动提交偏移量
            //      "session.timeout.ms" -> "30000", //session默认是30秒 超过5秒不提交offect就会报错
            //      "heartbeat.interval.ms" -> "5000", //10秒提交一次 心跳周期
            properties.put("group.id", groupId); //注意不同的流 group.id必须要不同 否则会出现offect commit提交失败的错误
            if (config.getOffsetMode().equalsIgnoreCase("earliest") || config.getOffsetMode().equalsIgnoreCase("latest")) {
                properties.put("auto.offset.reset", offsetMode); //latest   earliest
            }
            properties.put("enable.auto.commit", true); //latest   earliest
            properties.put("auto.commit.interval.ms", "500000");
            properties.put("connections.max.idle.ms", "5400000");
            properties.put("max.poll.interval.ms", "3000000");
            properties.put("fetch.max.wait.ms", "5000");
            properties.put("metadata.max.age.ms", "3000000");
//            properties.put("auto.commit.interval.ms", 1000); //latest   earliest
            if (config.getKrbs()) {
                properties.put("security.protocol", "SASL_PLAINTEXT");
                properties.put("sasl.mechanism", "GSSAPI");
                properties.put("sasl.kerberos.service.name", "kafka");
            }
            return properties;
        }

    }

    static class JsonDeserializationSchema
            implements KafkaDeserializationSchema<Row>
    {
        private static final ObjectMapper MAPPER = new ObjectMapper();
        private static TypeFactory typeFactory = TypeFactory.defaultInstance();

        private final RowTypeInfo rowTypeInfo;
        private final JsonDeserializer deserializer;
        private final Schema schema;

        public JsonDeserializationSchema(Schema schema)
        {
            this.schema = schema;
            this.rowTypeInfo = schemaToRowTypeInfo(schema);
            this.deserializer = new JsonDeserializer();
        }

        @Override
        public boolean isEndOfStream(Row nextElement)
        {
            return false;
        }

        @Override
        public Row deserialize(ConsumerRecord<byte[], byte[]> record)
        {
            deserializer.initNewMessage(record.value());

            Object[] values = new Object[schema.size()];
            for (int i = 0; i < schema.size(); i++) {
                Field field = schema.getField(i);
                switch (field.getName()) {
                    case "_topic":
                        values[i] = record.topic();
                        continue;
                    case "_message":
                        values[i] = new String(record.value(), UTF_8);
                        continue;
                    case "_key":
                        values[i] = record.key() == null ? null : new String(record.key(), UTF_8);
                        continue;
                    case "_partition":
                        values[i] = record.partition();
                        continue;
                    case "_offset":
                        values[i] = record.offset();
                        continue;
                }
                Object value = deserializer.deserialize(field);

                Type type = field.getJavaType();
                if (type instanceof MapType) {
                    MapType mapType = (MapType) type;

                    JavaType jType = typeFactory.constructMapType(mapType.getBaseClass(),
                            typeFactory.constructType(mapType.getKeyType()),
                            typeFactory.constructType(mapType.getValueType()));
                    value = MAPPER.convertValue(value, jType);
                }
                else if (type instanceof ArrayType) {
                    value = MAPPER.convertValue(value, typeFactory.constructType(((ArrayType) type).getValueType()));
                }
                else if (JavaTypes.typeToClass(type).isArray()) {
                    value = MAPPER.convertValue(value, JavaTypes.typeToClass(type));
                }
                else if (Number.class.isAssignableFrom(JavaTypes.typeToClass(type))) {
                    value = MAPPER.convertValue(value, JavaTypes.typeToClass(type));
                }
                values[i] = value;
            }
            return Row.of(values);
        }

        @Override
        public TypeInformation<Row> getProducedType()
        {
            return rowTypeInfo;
        }
    }

    private static class RowDeserializer
            implements KeyedDeserializationSchema<Row>
    {
        @Override
        public boolean isEndOfStream(Row nextElement)
        {
            return false;
        }

        @Override
        public Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset)
        {
            return Row.of(
                    topic, //topic
                    messageKey == null ? null : new String(messageKey, UTF_8), //key
                    new String(message, UTF_8), //message
                    partition,
                    offset
            );
        }

        @Override
        public TypeInformation<Row> getProducedType()
        {
            TypeInformation<?>[] types = new TypeInformation<?>[] {
                    TypeExtractor.createTypeInfo(String.class),
                    TypeExtractor.createTypeInfo(String.class), //createTypeInformation[String]
                    TypeExtractor.createTypeInfo(String.class),
                    Types.INT,
                    Types.LONG
            };
            return new RowTypeInfo(types, KAFKA_COLUMNS);
        }
    }
}
