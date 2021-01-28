package ideal.sylph.runner.flink.udf;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.shaded.org.joda.time.LocalDateTime;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CURRENT_DATE()
 * CURRENT_TIME()
 * INTEGER_VALUE()
 * CURRENT_TIMESTAMP()
 * LOCALTIME()
 * LOCALTIMESTAMP()
 */
@Name("NOW")
@Description("now -> bigint s or now(int s) -> bigint add s ")
public class Now extends ScalarFunction {

    @Description("BIGINT NOW()")
    public Long eval() {
        return LocalDateTime.now().toDateTime().getMillis();
    }

    /**
     * 当前时间增加 millis 微秒
     *
     * @param millis 增加的毫秒数
     * @return
     */
    @Description("BIGINT NOW(int millis)")
    public Long eval(Integer millis) {
        if (millis == null) {
            return null;
        }
        return LocalDateTime.now().plusMillis(millis).toDateTime().getMillis();
    }

}
