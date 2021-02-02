package ideal.sylph.plugins.kafka.flink.canal;

import com.google.common.base.Strings;

///**
// * Define standard column type for all the readers or writers that do not
// * have special types of their own
// *
// * @program: dataflow
// * @description:
// * @author: Mr.liutangrong
// * @create: 2019-07-19 14:24
// **/
public enum ColumnType {
    STRING,
    VARCHAR,
    CHAR,
    TEXT,
    ENUM,
    CLOB,
    BLOB,
    INT,
    INTEGER,
    MEDIUMINT,
    TINYINT,
    SMALLINT,
    BIGINT,
    DOUBLE,
    FLOAT,
    BOOLEAN,
    YEAR,
    DATE,
    DATETIME,
    TIME,
    TIMESTAMP,
    DECIMAL;

    public static ColumnType fromString(String type) {
        if (Strings.isNullOrEmpty(type)) {
            throw new RuntimeException("null ColumnType!");
        }
        String m_type = type.replaceAll("\\(.*", "").trim().toUpperCase();
        if (!Strings.isNullOrEmpty(m_type)) {
            switch (m_type) {
                case "BIT":
                    return BOOLEAN;
                case "REAL":
                    return FLOAT;
                case "NUMERIC":
                    return DECIMAL;
                case "UNSIGNED":
                    return BIGINT;
                case "":
                    return STRING;
                default:
                    return valueOf(m_type);
            }
        }
        return valueOf(type);
    }


}
