package ideal.sylph.plugins.kafka.flink.canal;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Objects;


/**
 * 类型转换工具类
 *
 * @author rewerma 2018-8-19 下午06:14:23
 * @version 1.0.0
 */
public class JdbcTypeUtil implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(JdbcTypeUtil.class);

    private final static String D_CODE_ISO = "ISO-8859-1";
    private final static String NULL_DATE = "0000-00-00";

    private final static String[] DATE_TIME_FORMAT = new String[]{
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss.sss",
            "yyyy-MM-dd HH:mm:ss.ss",
            "yyyy-MM-dd HH:mm:ss.s",
            "yyyy/MM/dd HH:mm:ss",
            "yyyyMMddHHmmss"
    };

    private final static String[] DATE_FORMAT = new String[]{
            "yyyy-MM-dd",
            "yyyy/MM/dd",
            "yyyyMMdd",
            "yyyy"
    };

    private final static String[] TIME_FORMAT = new String[]{
            "HHmmss",
            "HH:mm:ss"
    };

    /**
     * <P>将字段值按制定的JDBC类型转换
     * </P>
     *
     * @param value     字段值
     * @param mysqlType mysql类型（String）
     * @return
     */
    public static Object typeConvert(String value, String mysqlType) {
        Object result = null;
        if (Objects.isNull(value) || Objects.isNull(value.toString()) || StringUtils.isBlank(value.toString())) {
            return null;
        }
        ColumnType m_mySqlType = ColumnType.fromString(mysqlType);
        try {
           //  logger.debug("### value :{} , mysqlType :{}  ", value, mysqlType);
            switch (m_mySqlType) {
                case INT:
                case MEDIUMINT:
                case TINYINT:
                case INTEGER:
                case SMALLINT:
                    result = Integer.valueOf(value);
                    break;
//                case SMALLINT:
//                    result = Short.valueOf(value);
//                    break;
                case BIGINT:
                    result = Long.valueOf(value);
                    break;
                case BOOLEAN:
                    result = Boolean.valueOf(value);
                    break;
                case DOUBLE:
                    result = Double.valueOf(value);
                    break;
                case FLOAT:
                    result = Float.valueOf(value);
                    break;
                case DECIMAL:
                    result = new BigDecimal(value);
                    break;
                case BLOB:
                    result = value.getBytes(D_CODE_ISO);
                    break;
              /*  case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                */
                case YEAR:
                case DATE:
                    result = convertSqlDate(value);
                    break;
                case TIME:
                    result = convertSqlTime(value);
                    break;
                case DATETIME:
                case TIMESTAMP:
                    result = convertSqlTimestamp(value);
                    break;
                case ENUM://TODO 枚举解析
                    result = convertEnumAsString(value, mysqlType);
                    break;
                case CLOB:
                case CHAR:
                case TEXT:
                case VARCHAR:
                case STRING:
                default:
                    result = value;
            }
            return result;
        } catch (Exception e) {
            logger.warn("###  failed convert type {} to {}", value, mysqlType,e);
            return value;
        }
    }

    /**
     * fileType 转为TypeInformation2
     *
     * @param filedType
     * @return
     */
    public static TypeInformation<?> strConvertTypeInfo2(String filedType) {
        TypeInformation<?> result;
        ColumnType m_mySqlType = ColumnType.fromString(filedType);
        switch (m_mySqlType) {
            case INT:
            case MEDIUMINT:
                result = org.apache.flink.api.common.typeinfo.Types.INT;
                break;
            case SMALLINT:
                result = org.apache.flink.api.common.typeinfo.Types.SHORT;
                break;
            case TINYINT:
                result = org.apache.flink.api.common.typeinfo.Types.BYTE;
                break;
            case BIGINT:
            case INTEGER:
                result = org.apache.flink.api.common.typeinfo.Types.LONG;
                break;
            case BOOLEAN:
                result = org.apache.flink.api.common.typeinfo.Types.BOOLEAN;
                break;
            case DOUBLE:
                result = org.apache.flink.api.common.typeinfo.Types.DOUBLE;
                break;
            case FLOAT:
                result = org.apache.flink.api.common.typeinfo.Types.FLOAT;
                break;
            case DECIMAL:
                result = org.apache.flink.api.common.typeinfo.Types.BIG_DEC;
                break;
            case BLOB:
                result = org.apache.flink.api.common.typeinfo.Types.OBJECT_ARRAY(
                        org.apache.flink.api.common.typeinfo.Types.STRING);
                break;
            case YEAR:
            case DATE:
                result = org.apache.flink.api.common.typeinfo.Types.SQL_DATE;
                break;
            case TIME:
                result = org.apache.flink.api.common.typeinfo.Types.SQL_TIME;
                break;
            case DATETIME:
            case TIMESTAMP:
                result = org.apache.flink.api.common.typeinfo.Types.SQL_TIMESTAMP;
                break;
            case ENUM://TODO 枚举解析
            case CLOB:
            case CHAR:
            case TEXT:
            case VARCHAR:
            case STRING:
            default:
                result = org.apache.flink.api.common.typeinfo.Types.STRING;
        }
        return result;
    }


    /**
     * 将枚举转变为字符串
     *
     * @param value
     * @param mysqlType
     * @return
     */
    public static String convertEnumAsString(String value, String mysqlType) {
        if ("".equals(value) || "0".equals(value)) {
            return "";
        }
        try {
            String[] enums = mysqlType.replaceAll("enum\\(|ENUM\\(|\\)|'", "")
                    .split(",");
            Integer index = Integer.valueOf(value);
            return enums[index - 1];
        } catch (Exception e) {
            logger.warn("### 解析异常 value is :{}  mysqlType is :{},",value,mysqlType, e);
            return value;
        }
    }


    /**
     * 字符串转 Timestamp
     *
     * @param value
     * @return
     */
    public static Timestamp convertSqlTimestamp(String value) {
        Timestamp result = null;
        String m_value = value.trim();
        if ("".equals(m_value))
            return result;
        try {
            if (!value.startsWith(NULL_DATE)) {
                result = new Timestamp(
                        DateUtils.parseDate(m_value, DATE_TIME_FORMAT).getTime());
            }
        } catch (ParseException e) {
            logger.warn("### Timestamp value {} Unable to parse the date ", value);
        }
        return result;
    }

    /**
     * 时间字符串转为SQL time
     *
     * @param value
     * @return
     */
    public static Time convertSqlTime(String value) {
        Time result = null;
        String m_value = value.trim();
        if ("".equals(m_value))
            return result;
        try {
            result = new Time(
                    DateUtils.parseDate(m_value, TIME_FORMAT).getTime());
        } catch (ParseException e) {
            logger.warn("### Time value {} Unable to parse the date ", value,e);
        }
        return result;

    }


    /**
     * 字符串转 Timestamp
     *
     * @param value
     * @return
     */
    public static Date convertSqlDate(String value) {
        Date result = null;
        String m_value = value.trim();
        if ("".equals(m_value))
            return result;
        try {
            if (!value.startsWith(NULL_DATE)) {
                result = new Date(
                        DateUtils.parseDate(m_value, DATE_FORMAT).getTime());
            }
        } catch (ParseException e) {
            logger.warn("### date value {} Unable to parse the date ", value,e);
        }
        return result;
    }

}
