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
 * TIMESTAMP TO_TIMESTAMP(BIGINT time)
 * TIMESTAMP TO_TIMESTAMP(VARCHAR date)
 * TIMESTAMP TO_TIMESTAMP(VARCHAR date, VARCHAR format)
 */
@Name("TO_TIMESTAMP")
public class ToTimestamp extends ScalarFunction {
    private static final Logger logger = LoggerFactory.getLogger(ToTimestamp.class);

    private static final String DEFAULT_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    @Description("DATE TO_TIMESTAMP(BIGINT time) ")
    public Timestamp eval(Long time) {
        if (Objects.isNull(time)) {
            return null;
        }
        Timestamp reDateStr = null;
        try {
            reDateStr = new Timestamp((new DateTime(time)).getMillis());
        } catch (Exception e) {
            logger.warn(" TO_TIMESTAMP BIGINT time ", e);
        }
        return reDateStr;
    }

    @Description("DATE TO_TIMESTAMP(VARCHAR time) ")
    public Timestamp eval(String time) {
        if (Objects.isNull(time)) {
            return null;
        }
        Timestamp reDateStr = null;
        try {
            reDateStr = this.eval(time, DEFAULT_DATE_TIME_FORMAT);
        } catch (Exception e) {
            logger.warn(" TO_TIMESTAMP String date ", e);
        }
        return reDateStr;
    }


    @Description("DATE TO_TIMESTAMP(VARCHAR time,VARCHAR format) ")
    public Timestamp eval(String time, String format) {
        if (Objects.isNull(time) || Objects.isNull(format)) {
            return null;
        }
        Timestamp reDateStr = null;
        try {
            reDateStr = new Timestamp(DateTimeFormat.forPattern(format).parseDateTime(time).getMillis());
        } catch (Exception e) {
            logger.warn(" TO_TIMESTAMP(String date , VARCHAR format)", e);
        }
        return reDateStr;
    }
}
