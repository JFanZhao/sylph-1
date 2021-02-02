package ideal.sylph.plugins.kafka.flink.tibinlog.exception;

/**
 *
 */
public enum CommonErrorCode implements ErrorCode {

    FIELD_MISS("common-00", "解析错误,字段缺失 ."),
    FIELD_TYPE_CAST_ERROR("common-01", "解析错误，字段类型转换出错 ."),
    FIELD_TYPE_NOT_SUPPORT("common-02", "解析错误，字段类型不支持 .");

    private final String code;

    private final String describe;

    private CommonErrorCode(String code, String describe) {
        this.code = code;
        this.describe = describe;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.describe;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Describe:[%s]", this.code,
                this.describe);
    }

}
