package ideal.sylph.runner.flink.udf;


import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Objects;

/**
 * Date TO_DATE(INT time)
 * Date TO_DATE(Timestamp time)
 * Date TO_DATE(VARCHAR date)
 * Date TO_DATE(VARCHAR date,VARCHAR format)
 */
@Name("TO_DATE")
public class ToDate extends ScalarFunction {
    private static final Logger logger = LoggerFactory.getLogger(ToDate.class);

    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

    @Description("DATE TO_DATE(TIMESTAMP time)")
    public Date eval(Timestamp timestamp) {
        if (timestamp == null) {
            return null;
        }
        Date result = null;
        try {
            result = new Date(timestamp.getTime());
        } catch (Exception e) {
            logger.warn("TO_DATE 调用异常", e);
        }
        return result;
    }

    @Description("DATE TO_DATE(BIGINT time) ")
    public Date eval(Long time) {
        if (Objects.isNull(time)) {
            return null;
        }
        Date reDateStr = null;
        try {
            reDateStr = new Date((new DateTime(time)).getMillis());
        } catch (Exception e) {
            logger.warn(" TO_DATE BIGINT time ", e);
        }
        return reDateStr;
    }

    @Description("DATE TO_DATE(VARCHAR time) ")
    public Date eval(String time) {
        if (Objects.isNull(time)) {
            return null;
        }
        Date reDateStr = null;
        try {
            reDateStr = this.eval(time, DEFAULT_DATE_FORMAT);
        } catch (Exception e) {
            logger.warn(" TO_DATE String date ", e);
        }
        return reDateStr;
    }


    @Description("DATE TO_DATE(VARCHAR time,VARCHAR format) ")
    public Date eval(String time, String format) {
        if (Objects.isNull(time) || Objects.isNull(format)) {
            return null;
        }
        Date reDateStr = null;
        try {
            reDateStr = new Date(DateTimeFormat.forPattern(format).parseDateTime(time).getMillis());
        } catch (Exception e) {
            logger.warn(" TO_DATE String date , VARCHAR format ", e);
        }
        return reDateStr;
    }

}
