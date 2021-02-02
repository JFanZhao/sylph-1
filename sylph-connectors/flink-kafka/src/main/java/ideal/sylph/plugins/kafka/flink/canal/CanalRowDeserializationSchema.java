package ideal.sylph.plugins.kafka.flink.canal;

import com.alibaba.otter.canal.client.CanalMessageDeserializer;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.common.base.Strings;
import com.google.protobuf.InvalidProtocolBufferException;
import ideal.sylph.etl.Schema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ideal.sylph.runner.flink.engines.StreamSqlUtil.schemaToRowTypeInfo;


/**
 * @author My.liutangrong
 * @version 2.0
 * 将Canal数据解析成CanalRowsMessage消息格式，
 * 可以解析一张表并且指定字段顺序，也可以解析多个张表（但不能指定字段顺序）
 * <p>
 * Modify by Ivan on 2019.07.26 10:25
 */
public class CanalRowDeserializationSchema implements KeyedDeserializationSchema<List<CanalRowsMessage>> {
    private static final long serialVersionUID = 5532498435992063995L;
    final static Logger logger = Logger.getLogger(CanalRowDeserializationSchema.class);
    private List<String> files = new ArrayList<>();
    private String filterTableName;  // 需要筛选的表数据
    private boolean filterDDL = true; //默认过滤数据
    private long offset;
    private int partition;
    private final Schema scheme;

    /**
     * 1.解析指定表数据
     * 2.过滤指定ddl规则的数据
     * 3.row的字段顺序是按files字段顺序生成
     *
     * @param filterTableName 表名
     * @param files           字段顺序
     * @param filterDDL       是否过滤DDL对象
     */
    public CanalRowDeserializationSchema(String filterTableName, String[] files, boolean filterDDL, Schema schema) {
//        Objects.requireNonNull(filterTableName, "filter TableName list is not null ");
//        Objects.requireNonNull(files, "files array is not null ");
        this.files = Arrays.asList(files);
        this.filterTableName = filterTableName;
        this.filterDDL = filterDDL;
        this.scheme = schema;
    }

    @Override
    public List<CanalRowsMessage> deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
        List<CanalRowsMessage> flatMessages = new ArrayList<>();
        try {
            Message deserializer = CanalMessageDeserializer.deserializer(message);
            this.offset = offset;
            this.partition = partition;
            flatMessages = canalEntry2RowMessage(deserializer.getEntries());
        } catch (CanalClientException e) {
            //logger.info("### 信息：【{}】",Bytes.toString(message));
            logger.warn("### 序列化异常 ：{}", e);
            //throw new IOException(e);
        }
        return flatMessages;
    }


    /**
     * canalEntry转换成CanalRowMessage对象
     *
     * @param entries
     * @return
     */
    private List<CanalRowsMessage> canalEntry2RowMessage(List<CanalEntry.Entry> entries) {
        List<CanalRowsMessage> rowMessages = entries
                .stream()
                .filter(f -> isRowData(f))      // 过滤出实体数据，取出消息头和尾
                .filter(f -> filterTableMsg(f)) // 过滤对应表消息
                .map(f -> entryMapRowData(f))   // 解析消息
                .filter(f -> filterDdlMsg(f))// 过滤DDL对象
                .collect(Collectors.toList());
        return rowMessages;
    }

    /**
     * 过滤表数据
     *
     * @return
     */
    private boolean filterTableMsg(CanalEntry.Entry entry) {
        if (Strings.isNullOrEmpty(filterTableName)) {
            return true; //如果filterTableName为空  则返回所有数据
        } else {
            return filterTableName.equals(entry.getHeader().getTableName()) == true ? true : false;
        }

    }

    /**
     * 是否过滤DDL数据
     *
     * @param rowMsg
     * @return
     */
    private boolean filterDdlMsg(CanalRowsMessage rowMsg) {
        if (!filterDDL) {
            return true;//如果不过滤，返回全部数据
        } else {
            return rowMsg.getDdl() == true ? false : true;
        }

    }


    /**
     * 解析entry 转化为 CanalRowsMessage 对象
     *
     * @param entry
     * @return
     */
    private CanalRowsMessage entryMapRowData(CanalEntry.Entry entry) {
        CanalRowsMessage canalRow = new CanalRowsMessage();
        CanalEntry.Header header = entry.getHeader();
        canalRow.setGtid(header.getGtid()); //设置事务ID
        canalRow.setLogfileName(header.getLogfileName()); //binlog/redolog 文件名
        canalRow.setLogfileOffset(header.getLogfileOffset()); //binlog/redolog 文件的偏移位置
        canalRow.setDatabase(header.getSchemaName());
        canalRow.setTableName(header.getTableName());
        canalRow.setExecuteTime(header.getExecuteTime());
        try {
            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            Boolean isDdl = rowChange.getIsDdl();
            canalRow.setDdl(isDdl);
            if (isDdl) {
                canalRow.setSql(rowChange.getSql());
            } else {
                CanalEntry.EventType eventType = rowChange.getEventType();
                canalRow.setEventType(eventType.name());
                List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();
                for (CanalEntry.RowData rowData : rowDataList) {
                    List<CanalEntry.Column> columnsList = eventType == CanalEntry.EventType.DELETE ? rowData.getBeforeColumnsList() : rowData.getAfterColumnsList();
                    Tuple2<Row, List<String>> colAndFiled = columnsConvertRowOf(header,eventType,columnsList, files);
                    canalRow.setData(colAndFiled.f0);
                    canalRow.setFields(colAndFiled.f1);
                    canalRow.setPkNames(makePKNames(columnsList));
                    canalRow.setMysqlTypes(makeMysqlTypes(columnsList));
                    //canalRow.setMysqlIntTypes(makeIntMysqlTypes(columnsList));
                }
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return canalRow;
    }

    /**
     * 根据 files字段列表 判断是否需要按制定顺序解析
     *
     * @param columns
     * @param m_files
     * @return

    //TODO 减少遍历，将值和字段信息同时返回 需要优化
    private Tuple2<Row, List<String>> columnsConvertRowOf(List<CanalEntry.Column> columns,
                                                          List<String> m_files) {
        boolean convertType = m_files.isEmpty();

        if (convertType) {
            return new Tuple2<>(columnsConvertRow(columns), getFiles(columns, new ArrayList<>()));
        } else {
            return new Tuple2<>(columnsConvertRow(columns, m_files), m_files);
        }
    }
     */

    /**
     * 根据 files字段列表 判断是否需要按制定顺序解析
     *
     * @param columns
     * @param m_files
     * @return
     */
    //TODO 减少遍历，将值和字段信息同时返回 需要优化
    private Tuple2<Row, List<String>> columnsConvertRowOf(
            CanalEntry.Header header,
            CanalEntry.EventType eventType,
            List<CanalEntry.Column> columns,
            List<String> m_files) {
        boolean convertType = m_files.isEmpty();
        if (convertType) {
            return new Tuple2<>(columnsConvertRow(columns), getFiles(columns, new ArrayList<>()));
        } else {
            return new Tuple2<>(columnsConvertRow(columns, m_files,header,eventType), m_files);
        }
    }



    /**
     * 获取字段名称列表
     *
     * @param columns
     * @param files
     * @return
     */
    private List<String> getFiles(List<CanalEntry.Column> columns, List<String> files) {
        List<String> m_files;
        if (files.isEmpty()) {
            m_files = columns.stream().sorted((a, b) -> Integer.compare(a.getIndex(), b.getIndex())).
                    map(CanalEntry.Column::getName).collect(Collectors.toList());
        } else {
            m_files = files;
        }
        return m_files;
    }

    /**
     * 处理CanalEntry.Column数据
     *
     * @param columns
     * @return row
     */
    private Row columnsConvertRow(List<CanalEntry.Column> columns) {

        int size = Objects.requireNonNull(columns).size();
        Row row = new Row(size);
        columns.stream()
                .sorted((a, b) -> Integer.compare(a.getIndex(), b.getIndex()))
                .forEach(f -> {
                    row.setField(f.getIndex(), JdbcTypeUtil.typeConvert(f.getValue(), f.getMysqlType()));//f.getSqlType(),
                });

        return row;
    }

    /**
     * 处理CanalEntry.Column数据
     *
     * @param columns
     * @return row
     */
    private Row columnsConvertRow(List<CanalEntry.Column> columns
            , List<String> m_files
            , CanalEntry.Header header,
              CanalEntry.EventType eventType ) {
        Objects.requireNonNull(m_files, "files is not null");
        Map<String, CanalEntry.Column> data = columns.stream()
                .collect(Collectors.toMap(CanalEntry.Column::getName, Function.identity()));

        Stream<Object> row = m_files.stream()
                .map(f -> {
                    //增加获取partition和offset字段
                    if (f.equalsIgnoreCase("_offset")) {
                        return offset;
                    } else if (f.equalsIgnoreCase("_partition")) {
                        return partition;
                    } else if(f.equalsIgnoreCase("_database")){
                        // 数据库名称 string
                        return header.getSchemaName();
                    } else if(f.equalsIgnoreCase("_execute_time")){
                        //  数据变更时间 long
                        return header.getExecuteTime();
                    }else if(f.equalsIgnoreCase("_event_type")){
                        // 数据变更类型 int
                        return Objects.nonNull(eventType)==true ? eventType.getNumber():1;
                    }else if(f.equalsIgnoreCase("_event_type_name")){
                        // 数据变更类型名称 string
                        return Objects.nonNull(eventType)==true ? eventType.name():"";
                    }  else {
                        return JdbcTypeUtil.typeConvert(data.get(f).getValue(), data.get(f).getMysqlType());
                    }
                });//data.get(f).getSqlType(),
        return Row.of(row.toArray());

    }


    /**
     * 处理CanalEntry.Column,提取PkNames数据
     *
     * @param columns
     * @return
     */
    private List<String> makePKNames(List<CanalEntry.Column> columns) {
        return columns
                .stream()
                .filter(CanalEntry.Column::getIsKey)
                .map(CanalEntry.Column::getName)
                .collect(Collectors.toList());
    }

    /**
     * 处理CanalEntry.Column，提取MysqlTypes数据
     *
     * @param columns
     * @return
     */
    private Map<String, String> makeMysqlTypes(List<CanalEntry.Column> columns) {
        return columns
                .stream()
                .collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getMysqlType));
    }

    /**
     * 处理CanalEntry.Column，提取MysqlTypes数据
     *
     * @param columns
     * @return
     */
    private Map<String, Integer> makeIntMysqlTypes(List<CanalEntry.Column> columns) {
        return columns
                .stream()
                .collect(Collectors.toMap(CanalEntry.Column::getName, CanalEntry.Column::getSqlType));
    }

    /**
     * 是否为数据行
     */
    private Boolean isRowData(CanalEntry.Entry entry) {
        return !(entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN ||
                entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND);
    }

    @Override
    public boolean isEndOfStream(List<CanalRowsMessage> nextElement) {
        return false;
    }

    @Override
    public TypeInformation<List<CanalRowsMessage>> getProducedType() {
        return TypeInformation.of(new TypeHint<List<CanalRowsMessage>>() {
            @Override
            public TypeInformation<List<CanalRowsMessage>> getTypeInfo() {
                return super.getTypeInfo();
            }
        });
    }


}
