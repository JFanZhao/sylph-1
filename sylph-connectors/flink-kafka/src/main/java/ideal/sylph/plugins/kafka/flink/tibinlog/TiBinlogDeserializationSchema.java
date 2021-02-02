package ideal.sylph.plugins.kafka.flink.tibinlog;


import ideal.sylph.etl.Schema;
import ideal.sylph.plugins.kafka.flink.canal.JdbcTypeUtil;
import ideal.sylph.plugins.kafka.flink.tibinlog.exception.CommonErrorCode;
import ideal.sylph.plugins.kafka.flink.tibinlog.proto.BinLogInfo;
import javafx.util.Pair;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static ideal.sylph.runner.flink.engines.StreamSqlUtil.schemaToRowTypeInfo;

public class TiBinlogDeserializationSchema implements KeyedDeserializationSchema<List<Row>> {
    private static Logger logger = LoggerFactory.getLogger(TiBinlogDeserializationSchema.class);

    private String filterTableName;  // 需要筛选的表数据
    private final Schema schema;
    private final RowTypeInfo rowTypeInfo;

    /**
     * 构造函数
     *
     * @param filterTableName
     * @param schema
     */
    public TiBinlogDeserializationSchema(String filterTableName, Schema schema) {
        this.filterTableName = filterTableName;
        this.schema = schema;
        this.rowTypeInfo = schemaToRowTypeInfo(schema);

    }

    /**
     * 反序列化函数
     *
     * @param messageKey
     * @param message
     * @param topic
     * @param partition
     * @param offset
     * @return
     */
    @Override
    public List<Row> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {
        try {
            BinLogInfo.Binlog binlogMsg = dealMessage(message);
//            if (logger.isDebugEnabled()) {
//                String deBugMsg = String.format("解析TiDB Binlog消息:[topic: %s,partition: %d, offset %d ,msg:%s ]"
//                        , topic, partition, offset, binlogMsg.toString());
//                logger.debug(deBugMsg);
//            }
            Optional<BinLogInfo.Binlog> dmlBinlogMsg = getDmlMessage(binlogMsg);
            List<BinLogInfo.Table> tableMsg = getTableMessage(dmlBinlogMsg, filterTableName);
            if (tableMsg.isEmpty()) {
                // 表不存在，直接返回
                return Collections.emptyList();
            }
            // 解析 转为rows
            List<Row> rows = tableMsg.stream().flatMap(f -> {
                return f.getMutationsList().stream().map(item -> {
                    CastRowVo castRowVo = new CastRowVo(topic, partition, offset, dmlBinlogMsg.get().getCommitTs(), f);
                    return mutationsToRows(item, f.getColumnInfoList(), rowTypeInfo, castRowVo);
                });
            }).collect(Collectors.toList());
            return rows;
        } catch (Exception e) {
            logger.warn("解析TiDB Binlog 消息日志错误, topic: {},partition: {}, offset :{} ", topic, partition, offset, e);
        }
        return Collections.emptyList();
    }


    /**
     * 解析数据
     *
     * @param value
     * @return
     * @throws Exception
     */
    private BinLogInfo.Binlog dealMessage(byte[] value) throws Exception {
        BinLogInfo.Binlog binlog = BinLogInfo.Binlog.parseFrom(value);
//        if (logger.isDebugEnabled()) {
//            logger.debug(String.format("解析数据：%s", binlog.toString()));
//        }
        return binlog;
    }

    /**
     * 获取DML对象
     *
     * @param binlog
     * @return
     */
    private Optional<BinLogInfo.Binlog> getDmlMessage(BinLogInfo.Binlog binlog) {
        BinLogInfo.BinlogType binlogType = binlog.getType();
        if (binlogType.getNumber() == 0)
            return Optional.of(binlog);
        else
            return Optional.empty();
    }

    /**
     * 获取表数据对象
     * 从数据库里过滤出对应的表数据
     *
     * @param binlog
     * @param table
     * @return
     */
    private List<BinLogInfo.Table> getTableMessage(Optional<BinLogInfo.Binlog> binlog, String table) {
        if (binlog.isPresent()) {
            List<BinLogInfo.Table> tableMag = binlog.get()
                    .getDmlData()
                    .getTablesList()
                    .stream()
                    .filter(f -> f.getTableName().equals(table)).collect(Collectors.toList());
            return tableMag;
        }
        return Collections.emptyList();
    }

    /**
     * 数据实体转为Row
     * 掉用类型转化，将原始数据转化为目标的类型
     *
     * @param tableMutation
     * @param columnInfoList
     * @param castRowVo
     * @return
     */
    private Row mutationsToRows(BinLogInfo.TableMutation tableMutation
            , List<BinLogInfo.ColumnInfo> columnInfoList
            , RowTypeInfo rowTypeInfo
            , CastRowVo castRowVo) {

        BinLogInfo.MutationType rowEventType = tableMutation.getType();
        String[] fields = rowTypeInfo.getFieldNames();
        TypeInformation<?>[] types = rowTypeInfo.getFieldTypes();
        int rowSize = fields.length;
        int colInfoSize = columnInfoList.size();
        Row row = new Row(rowSize);
        // 不用判断 事件类型 ，直接取变更后的数据
        BinLogInfo.Row itemRow = tableMutation.getRow();
        // fields -> columns,columns info
        Map<String, Pair<BinLogInfo.ColumnInfo, BinLogInfo.Column>> rowMap = new ConcurrentHashMap<>();
        for (int i = 0; i < colInfoSize; i++) {
            BinLogInfo.ColumnInfo colInfo = columnInfoList.get(i);
            BinLogInfo.Column col = itemRow.getColumnsList().get(i);
            rowMap.put(colInfo.getName().toLowerCase(), new Pair(colInfo, col));
        }
        //解析数据，并给row赋值。
        for (int i = 0; i < rowSize; i++) {
            String field = fields[i];

            if (field.equalsIgnoreCase("_offset")) {
                row.setField(i, castRowVo.getOffset());
                continue;
            } else if (field.equalsIgnoreCase("_partition")) {
                row.setField(i, castRowVo.getPartition());
                continue;
            } else if (field.equalsIgnoreCase("_database")) {
                row.setField(i, castRowVo.getTable().getSchemaName());
                continue;
            } else if (field.equalsIgnoreCase("_execute_time")) {
                row.setField(i, castRowVo.getExecuteTime());
                continue;
            } else if (field.equalsIgnoreCase("_event_type")) {
                row.setField(i, rowEventType.getNumber());
                continue;
            } else if (field.equalsIgnoreCase("_event_type_name")) {
                if (BinLogInfo.MutationType.Insert.equals(rowEventType)) {
                    row.setField(i, "INSERT");
                } else if (BinLogInfo.MutationType.Update.equals(rowEventType)) {
                    row.setField(i, "UPDATE");
                } else {
                    row.setField(i, "DELETE");
                }
                continue;
            }

            Pair<BinLogInfo.ColumnInfo, BinLogInfo.Column> fieldData = rowMap.getOrDefault(field.toLowerCase(), null);
            if (Objects.isNull(fieldData)) {
                logger.warn("字段：{} , 异常信息：{} ", field, CommonErrorCode.FIELD_MISS.toString());
                row.setField(i, null);
                continue;
            } else {
                row.setField(i, fieldTypeCast(types[i], fieldData));
            }
        }
        rowMap.clear();
        return row;
    }


    /**
     * 字段值转换为flink 目标类型
     *
     * @param toFieldType
     * @param fieldData
     * @return
     */
    private Object fieldTypeCast(TypeInformation<?> toFieldType
            , Pair<BinLogInfo.ColumnInfo, BinLogInfo.Column> fieldData) {
        Object value = null;
        Object tiDbValue = getTiDbBinlogTypeValue(fieldData);
        if (Objects.isNull(tiDbValue)) {
            return value;
        }
        try {
            if (toFieldType.getTypeClass().equals(BasicTypeInfo.STRING_TYPE_INFO.getTypeClass())) {
                return String.valueOf(tiDbValue);
            } else if (toFieldType.getTypeClass().equals(BasicTypeInfo.LONG_TYPE_INFO.getTypeClass())) {
                return Long.valueOf(tiDbValue.toString());
            } else if (toFieldType.getTypeClass().equals(BasicTypeInfo.SHORT_TYPE_INFO.getTypeClass())) {
                return Short.valueOf(tiDbValue.toString());
            } else if (toFieldType.getTypeClass().equals(BasicTypeInfo.INT_TYPE_INFO.getTypeClass())) {
                return Integer.valueOf(tiDbValue.toString());
            } else if (toFieldType.getTypeClass().equals(SqlTimeTypeInfo.DATE.getTypeClass())) {
                return JdbcTypeUtil.convertSqlDate(tiDbValue.toString());
            } else if (toFieldType.getTypeClass().equals(SqlTimeTypeInfo.TIME.getTypeClass())) {
                return JdbcTypeUtil.convertSqlTime(tiDbValue.toString());
            } else if (toFieldType.getTypeClass().equals(SqlTimeTypeInfo.TIMESTAMP.getTypeClass())) {
                String msqlType = fieldData.getKey().getMysqlType().toLowerCase();
                // 对不同的时间类型做转换
                if ("datetime".equals(msqlType)) {
                    return JdbcTypeUtil.convertSqlTimestamp(tiDbValue.toString());
                } else if ("timestamp".equals(msqlType)) {
                    return JdbcTypeUtil.convertSqlTimestamp(tiDbValue.toString());
                }
                return tiDbValue;
            } else if (toFieldType.getTypeClass().equals(BasicTypeInfo.DATE_TYPE_INFO.getTypeClass())) {
                String msqlType = fieldData.getKey().getMysqlType().toLowerCase();
                // 对不同的时间类型做转换
                if ("datetime".equals(msqlType)) {
                    return JdbcTypeUtil.convertSqlTimestamp(tiDbValue.toString());
                } else if ("time".equals(msqlType)) {
                    return JdbcTypeUtil.convertSqlTime(tiDbValue.toString());
                } else if ("date".equals(msqlType)) {
                    return JdbcTypeUtil.convertSqlDate(tiDbValue.toString());
                } else if ("timestamp".equals(msqlType)) {
                    return JdbcTypeUtil.convertSqlTimestamp(tiDbValue.toString());
                }
                return tiDbValue;
            } else if (toFieldType.getTypeClass().equals(BasicTypeInfo.BIG_INT_TYPE_INFO.getTypeClass())) {
                return new BigInteger(tiDbValue.toString());
            } else if (toFieldType.getTypeClass().equals(BasicTypeInfo.FLOAT_TYPE_INFO.getTypeClass())) {
                return Float.valueOf(tiDbValue.toString());
            } else if (toFieldType.getTypeClass().equals(BasicTypeInfo.BIG_DEC_TYPE_INFO.getTypeClass())) {
                return new BigDecimal(tiDbValue.toString());
            } else if (toFieldType.getTypeClass().equals(BasicTypeInfo.DOUBLE_TYPE_INFO.getTypeClass())) {
                return Double.valueOf(tiDbValue.toString());
            } else if (toFieldType.getTypeClass().equals(BasicTypeInfo.BOOLEAN_TYPE_INFO.getTypeClass())) {
                return Boolean.valueOf(tiDbValue.toString());
            }

        } catch (Exception e) {
            BinLogInfo.ColumnInfo colInfo = fieldData.getKey();
            logger.warn("Tidb数据转换为Flink类型错误，字段：{}, 值：{} ,原始类型: {} , Flink类型：{}",
                    colInfo.getName(), tiDbValue, colInfo.getMysqlType(), toFieldType.getTypeClass(), e);
        }
        if (fieldData.getKey().getName().equals("item_snapshot_id") && tiDbValue == null){
            System.out.println("o");
        }
        return tiDbValue;
    }

    /**
     * 根据Tidb的类型解析字段的值，获取对应的值返回
     *
     * @return
     */
    private Object getTiDbBinlogTypeValue(Pair<BinLogInfo.ColumnInfo, BinLogInfo.Column> fieldData) {
        Object value = null;
        String mType = fieldData.getKey().getMysqlType();
        BinLogInfo.Column column = fieldData.getValue();

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("字段：%s ,字段类型：%s .",
                    fieldData.getKey().getName(), fieldData.getKey().getMysqlType()));
        }
        /**
         * 数据为null，直接返回
         */
        if (column.getIsNull()) {
            return value;
        }

        switch (mType.toLowerCase()) {
            case "date":
            case "timestamp":
            case "datetime":
            case "time":
            case "text":
            case "char":
            case "varchar":
            case "decimal":
                value = column.getStringValue();
                break;
            case "tinyint":
            case "mediumint":
            case "smallint":
            case "enum":
                value = null;
                if (column.hasUint64Value()) {
                    value = ((Long) column.getUint64Value()).intValue();
                } else if (column.hasInt64Value()) {
                    value = ((Long) column.getInt64Value()).intValue();
                } else if (column.hasStringValue()) {
                    value = Integer.valueOf(column.getStringValue());
                }
                break;
            case "int":
            case "long":
            case "uint":
            case "set":
            case "bigint":
                if (column.hasUint64Value()) {
                    value = column.getUint64Value();
                } else if (column.hasInt64Value()) {
                    value = column.getInt64Value();
                }
                break;
            case "float":
            case "double":
                value = column.getDoubleValue();
                break;
            case "bit":
            case "binary":
            case "blob":
                value = column.getBytesValue();
                break;
            case "json":
                value = column.getBytesValue().toStringUtf8();
                break;
            default:
                logger.warn("字段：{} ,字段类型：{} ,类型不支持，请联系大数据团队 .",
                        fieldData.getKey().getName(), fieldData.getKey().getMysqlType());
        }
        if (fieldData.getKey().getName().equals("item_snapshot_id") && value == null){
            System.out.println("o");
        }
        return value;
    }


    @Override
    public boolean isEndOfStream(List<Row> nextElement) {
        return false;
    }

    /**
     * TypeInformation 类型字段信息
     *
     * @return
     */
    @Override
    public TypeInformation<List<Row>> getProducedType() {
        return TypeInformation.of(new TypeHint<List<Row>>() {
            @Override
            public TypeInformation<List<Row>> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
    }


    private static class CastRowVo {
        private String topic;
        private int partition;
        private long offset;
        private long executeTime;
        private BinLogInfo.Table table;

        CastRowVo(String topic, int partition, long offset, long executeTime, BinLogInfo.Table table) {
            this.executeTime = executeTime;
            this.offset = offset;
            this.partition = partition;
            this.table = table;
            this.topic = topic;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getPartition() {
            return partition;
        }

        public void setPartition(int partition) {
            this.partition = partition;
        }

        public long getOffset() {
            return offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public long getExecuteTime() {
            return executeTime;
        }

        public void setExecuteTime(long executeTime) {
            this.executeTime = executeTime;
        }

        public BinLogInfo.Table getTable() {
            return table;
        }

        public void setTable(BinLogInfo.Table table) {
            this.table = table;
        }
    }
}
