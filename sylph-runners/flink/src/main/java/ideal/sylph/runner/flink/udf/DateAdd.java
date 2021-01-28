package ideal.sylph.runner.flink.udf;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;


import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * 对时间进行加
 * 单位是：天
 * VARCHAR DATE_ADD(VARCHAR startdate, INT days)
 * VARCHAR DATE_ADD(TIMESTAMP time, INT days)
 */
@Name("DATE_ADD")
public class DateAdd extends ScalarFunction {
    private static final Logger logger = LoggerFactory.getLogger(DateAdd.class);

    private static final String DEFAULT_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

    @Description("VARCHAR DATE_ADD(TIMESTAMP time, INT days)")
    public String eval(Timestamp timestamp, Integer days) {
        if (timestamp == null || days == null) {
            return null;
        }
        String result = null;
        try {
            result = new DateTime(timestamp).plusDays(days).toString(DEFAULT_DATE_FORMAT);
        } catch (Exception e) {
            logger.warn("DATE_ADD 调用异常", e);
        }
        return result;
    }

    @Description("VARCHAR DATE_ADD(TIMESTAMP time,VARCHAR to_format, INT days)")
    public String eval(Timestamp timestamp, String toFormat, Integer days) {
        if (timestamp == null || days == null) {
            return null;
        }
        String result = null;
        try {
            result = new DateTime(timestamp).plusDays(days).toString(toFormat);
        } catch (Exception e) {
            logger.warn("DATE_ADD 调用异常", e);
        }
        return result;
    }


    @Description("VARCHAR DATE_ADD(VARCHAR time, INT days)")
    public String eval(String date, Integer days) {
        boolean isReNull = Objects.isNull(date) || Objects.equals(date, "") || Objects.isNull(days);
        if (isReNull) {
            return null;
        }
        String result = null;
        try {
            result = this.eval(date, DEFAULT_DATE_FORMAT, days);

        } catch (Exception e) {
            logger.warn("DATE_ADD 调用异常", e);
        }
        return result;
    }

    @Description("VARCHAR DATE_ADD(VARCHAR time,VARCHAR to_format, INT days)")
    public String eval(String date, String toFormat, Integer days) {
        boolean isReNull = Objects.isNull(date) || Objects.equals(date, "") || Objects.isNull(days);
        if (isReNull) {
            return null;
        }
        String result = null;
        try {
            result = this.eval(date, DEFAULT_DATE_TIME_FORMAT, toFormat, days);
        } catch (Exception e) {
            logger.warn("DATE_ADD 调用异常", e);
        }
        return result;
    }


    @Description("VARCHAR DATE_ADD(VARCHAR time,VARCHAR from_format, VARCHAR to_format, INT days)")
    public String eval(String date, String fromFormat, String toFormat, Integer days) {
        boolean isReNull = Objects.isNull(date) || Objects.equals(date, "") || Objects.isNull(days);
        if (isReNull) {
            return null;
        }
        String result = null;
        try {
            result = DateTimeFormat.forPattern(fromFormat).parseDateTime(date).plusDays(days).toString(toFormat);
        } catch (Exception e) {
            logger.warn("DATE_ADD 调用异常", e);
        }
        return result;
    }


}
