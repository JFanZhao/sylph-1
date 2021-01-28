package ideal.sylph.runner.flink.udf;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.table.shaded.org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Objects;

@Name("HOUR")
public class Hour extends ScalarFunction {
    private static final Logger logger = LoggerFactory.getLogger(Hour.class);
    
    @Description("BIGINT HOUR(Time time)")
    public Long eval(Time time) {
        if (time == null) {
            return null;
        }
        Long reHour = null;
        try {
            reHour = Long.valueOf((new DateTime(time)).hourOfDay().get());
        } catch (Exception e) {
            logger.warn("HOUR 运行异常：", e);
        }
        return reHour;
    }

    @Description("BIGINT HOUR(TIMESTAMP timestamp)")
    public Long eval(Timestamp time) {
        if (time == null) {
            return null;
        }
        Long reHour = null;
        try {
            reHour = Long.valueOf((new DateTime(time)).hourOfDay().get());
        } catch (Exception e) {
            logger.warn("HOUR 运行异常：", e);
        }
        return reHour;
    }

    @Description("BIGINT HOUR(String date, String fromFormat) ")
    public Long eval(String date, String fromFormat) {
        if (Objects.isNull(date) || Objects.isNull(fromFormat)) {
            return null;
        }
        Long reHour = null;
        try {
            reHour = Long.valueOf(DateTimeFormat.forPattern(fromFormat).parseDateTime(date).hourOfDay().get());
        } catch (Exception e) {
            logger.warn("HOUR 运行异常：", e);
        }
        return reHour;
    }

}
