package ideal.sylph.runner.flink.udf;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

@Name("FROM_UNIXTIME")
public class FromUnixTime extends ScalarFunction {
    private static final Logger logger = LoggerFactory.getLogger(FromUnixTime.class);
    private static final String DEFAULT_DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /*    @Override
        public TypeInformation<?> getResultType(Class<?>[] signature) {
            return signature.length == 2 ? Types.STRING : Types.SQL_TIMESTAMP;
        }*/

    @Description("VARCHAR FROM_UNIXTIME(BIGINT time) ")
    public String eval(Long time) {
        return this.eval(time, DEFAULT_DATE_TIME_FORMAT);
    }

    @Description("VARCHAR FROM_UNIXTIME(BIGINT time, String toFormat) ")
    public String eval(Long time, String format) {
        if (Objects.isNull(time) || Objects.isNull(format)) {
            return null;
        }
        String reDateStr = null;
        try {
            reDateStr = (new DateTime(time)).toString(format);
        } catch (Exception e) {
            logger.warn(" FROM_UNIXTIME(BIGINT time, String toFormat)", e);
        }
        return reDateStr;
    }


}
