package ideal.sylph.runner.flink.udf;

import ideal.sylph.annotation.Name;
import java.text.ParseException;
import java.util.Arrays;
import	java.sql.Date;
import	java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

///**
// * @ClassName: StrToDate
// * @Description: TODO
// * @Author: Yangchong
// * @Data: 2019/9/20 16:06
// * @Version: 1.0
// **/
@Name("str_to_date")
public class StrToDate  extends ScalarFunction {
    private static Logger logger = LoggerFactory.getLogger(StrToDate.class);

    private final static String[] DATE_TIME_FORMAT = new String [] {
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss.sss",
            "yyyy-MM-dd HH:mm:ss.ss",
            "yyyy-MM-dd HH:mm:ss.s",
            "yyyy/MM/dd HH:mm:ss",
            "yyyyMMddHHmmss",
            "yyyy-MM-dd'T'HH:mm:ss.sss+08:00"
    };

    private final static String[] DATE_FORMAT = new String [] {
            "yyyy-MM-dd",
            "yyyy/MM/dd",
            "yyyyMMdd",
            "yyyy"
    };

    private final static String[] TIME_FORMAT = new String [] {
            "HHmmss",
            "HH:mm:ss"
    };

    public StrToDate() {
        super();
    }

    /**数据类型转化
     * 只将string类型的转化为Timestamp类型
     * @param timestamp
     * @return
     */
    public Timestamp eval(String timestamp) {
        Timestamp result = null;
        if(StringUtils.isEmpty(timestamp)){
            return result;
        }
        List<String> TimestampFormat = Arrays.asList(DATE_TIME_FORMAT);
        try {
            result = new Timestamp(
                    DateUtils.parseDate(timestamp, DATE_TIME_FORMAT).getTime());
        } catch (ParseException e) {
            logger.error("### Timestamp value {} Unable to parse the Timestamp type  ,{}", timestamp,e);
        }
        return result;
    }

    /**数据类型转化
     * 只将string类型的转化为Date类型
     * @param date
     * @return
     */
    public Date eval(String date,boolean nul) {
        Date result = null;
        if(StringUtils.isEmpty(date)){
            return result;
        }
        try {
            result = new Date(
                    DateUtils.parseDate(date, DATE_FORMAT).getTime());
        } catch (ParseException e) {
            logger.error("### date value {} Unable to parse the Date type  ,{}", date,e);
        }
        return result;
    }

    /**数据类型转化
     * 只将string类型的转化为Time类型
     * @param time
     * @return
     */
    public Time eval(String time, int nul) {
        Time result = null;
        if(StringUtils.isEmpty(time)){
            return result;
        }
        try {
            result = new Time(
                    DateUtils.parseDate(time, TIME_FORMAT).getTime());
        } catch (ParseException e) {
            logger.error("### date value {} Unable to parse the Date type  ,{}", time,e);
        }
        return result;
    }
}
