package ideal.sylph.runner.flink.udf;

import ideal.sylph.annotation.Name;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.IOException;

/**
 * substring_index implement
 */
@Name("substring_index")
public class SubStringIndex extends ScalarFunction {

    public String eval(String field, String delimiter, int index) throws IOException {
        String result = field;

        if (field == null || field.trim().length() == 0) {
            return result;
        }
        if (index == 0) {
            return "";
        }
        if (index < 0) {
            field = new StringBuffer(field).reverse().toString();
        }
        int beginIndex = 0;
        int count = 0;
        while ((beginIndex = field.indexOf(delimiter, beginIndex)) != -1) {
            count++;
            if (count == Math.abs(index)) {
                if (index < 0) {
                    field = new StringBuffer(field).reverse().toString();
                    result = field.substring(field.length() - beginIndex);
                } else {
                    result = field.substring(0, beginIndex);
                }
                return result;
            }
            beginIndex = beginIndex + delimiter.length();
        }
        return result;
    }

}
