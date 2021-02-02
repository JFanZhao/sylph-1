package ideal.sylph.plugins.kafka.flink.tibinlog.exception;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * 字段解析异常类
 */
public class FieldMissingException extends RuntimeException{

    private static final long serialVersionUID = 1L;

    private ErrorCode errorCode;

    public FieldMissingException(ErrorCode errorCode, String errorMessage) {
        super(errorCode.toString() + " - " + errorMessage);
        this.errorCode = errorCode;
    }

    private FieldMissingException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode.toString() + " - " + getMessage(errorMessage) + " - " + getMessage(cause), cause);

        this.errorCode = errorCode;
    }

    public static FieldMissingException asFieldMissingException(ErrorCode errorCode, String message) {
        return new FieldMissingException(errorCode, message);
    }

    public static FieldMissingException asFieldMissingException(ErrorCode errorCode, String message, Throwable cause) {
        if (cause instanceof FieldMissingException) {
            return (FieldMissingException) cause;
        }
        return new FieldMissingException(errorCode, message, cause);
    }

    public static FieldMissingException asFieldMissingException(ErrorCode errorCode, Throwable cause) {
        if (cause instanceof FieldMissingException) {
            return (FieldMissingException) cause;
        }
        return new FieldMissingException(errorCode, getMessage(cause), cause);
    }

    public ErrorCode getErrorCode() {
        return this.errorCode;
    }

    private static String getMessage(Object obj) {
        if (obj == null) {
            return "";
        }

        if (obj instanceof Throwable) {
            StringWriter str = new StringWriter();
            PrintWriter pw = new PrintWriter(str);
            ((Throwable) obj).printStackTrace(pw);
            return str.toString();
        } else {
            return obj.toString();
        }
    }

}
