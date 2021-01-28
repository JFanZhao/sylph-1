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

@Name("DATE_FORMAT")
public class DateFormat extends ScalarFunction {
    private static final Logger logger = LoggerFactory.getLogger(DateFormat.class);
    private static final String DEFAULT_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  /*  @Override
    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Arrays.equals(signature, new Class[]{String.class, String.class}) ? Types.SQL_TIMESTAMP : Types.STRING;
    }*/

    @Description("TIMESTAMP DATE_FORMAT(String time, String toFormat) ")
    public String eval(String time, String toFormat) {
        return this.eval(time, DEFAULT_DATE_TIME_FORMAT, toFormat);
    }

    @Description("String DATE_FORMAT(TIMESTAMP time, String toFormat) ")
    public String eval(Timestamp time, String toFormat) {
        if (Objects.isNull(time) || Objects.isNull(toFormat)) {
            return null;
        }
        String returnDateStr = null;
        try {
            returnDateStr = (new DateTime(time)).toString(toFormat);
        } catch (Exception e) {
            logger.warn(" DATE_FORMAT var2 of java.sql.Date", e);
        }
        return returnDateStr;
    }

    @Description("VARCHAR DATE_FORMAT(DATE date, String toFormat) ")
    public String eval(java.sql.Date date, String toFormat) {
        if (Objects.isNull(date) || Objects.isNull(toFormat)) {
            return null;
        }
        String returnDateStr = null;
        try {
            returnDateStr = (new DateTime(date)).toString(toFormat);
        } catch (Exception e) {
            logger.warn(" DATE_FORMAT var2 of java.sql.Date", e);
        }
        return returnDateStr;
    }

    @Description("VARCHAR DATE_FORMAT(VARCHAR date, VARCHAR from_format, VARCHAR to_format) ")
    public String eval(String time, String fromFormat, String toFormat) {
        boolean isReturnNull = Objects.isNull(time) || Objects.isNull(fromFormat) || Objects.isNull(toFormat);
        if (isReturnNull) {
            return null;
        }
        String returnDateStr = null;
        try {
            returnDateStr = DateTimeFormat.forPattern(fromFormat).parseDateTime(time).toString(toFormat);
        } catch (Exception e) {
            logger.warn(" DATE_FORMAT var3 exception", e);
        }
        return returnDateStr;
    }
}

