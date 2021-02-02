package ideal.sylph.plugins.kafka.flink.canal;

import org.apache.flink.types.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

///**
// * @Created 2019-06-28
// * @Author David
// * @Summary presto entry class
// */
public class CanalRowsMessage implements Serializable {

    private static final long serialVersionUID = 4334757085596792911L;

    private String gtid;  //事务ID
    private String logfileName; // binlog/redolog 文件名
    private long logfileOffset = 0; // binlog/redolog 文件的偏移位置
    private String database; //数据库名称
    private String tableName; //表
    private long executeTime = 0;  //数据变更时间
    private Boolean isDdl = false; //标识是否是ddl语句
    private String sql; //ddl/query的sql语句
    private String eventType; //数据变更类型
    private List<String> pkNames = new ArrayList<String>();//主键
    private Map<String, String> mysqlTypes = new HashMap<String, String>();//字段信息
    //private Map<String, Integer> mysqlIntTypes = new HashMap<String, Integer>();//字段信息
    private List<String> fields;
    private Row data; //字段json

    public String getGtid() {
        return gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public String getLogfileName() {
        return logfileName;
    }

    public void setLogfileName(String logfileName) {
        this.logfileName = logfileName;
    }

    public long getLogfileOffset() {
        return logfileOffset;
    }

    public void setLogfileOffset(long logfileOffset) {
        this.logfileOffset = logfileOffset;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
    }

    public Boolean getDdl() {
        return isDdl;
    }

    public void setDdl(Boolean ddl) {
        isDdl = ddl;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public Map<String, String> getMysqlTypes() {
        return mysqlTypes;
    }

    public void setMysqlTypes(Map<String, String> mysqlTypes) {
        this.mysqlTypes = mysqlTypes;
    }

    public Row getData() {
        return data;
    }

    public void setData(Row data) {
        this.data = data;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

/*    public Map<String, Integer> getMysqlIntTypes() {
        return mysqlIntTypes;
    }

    public void setMysqlIntTypes(Map<String, Integer> mysqlIntTypes) {
        this.mysqlIntTypes = mysqlIntTypes;
    }*/

    @Override
    public String toString() {
        return "FlatMessage{" +
                "gtid='" + gtid + '\'' +
                ", logfileName='" + logfileName + '\'' +
                ", logfileOffset=" + logfileOffset +
                ", database='" + database + '\'' +
                ", tableName='" + tableName + '\'' +
                ", executeTime=" + executeTime +
                ", isDdl=" + isDdl +
                ", sql='" + sql + '\'' +
                ", eventType='" + eventType + '\'' +
                ", pkNames=" + pkNames +
                ", mysqlTypes=" + mysqlTypes +
                //", mysqlIntTypes=" + mysqlIntTypes +
                ", fields =" + fields +
                ", data='" + data.toString() + '\'' +
                '}';
    }

}
