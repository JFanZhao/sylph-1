/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ideal.sylph.plugins.kafka.flink;

import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.etl.PluginConfig;

import java.util.Date;

public class KafkaSourceConfig
        extends PluginConfig
{
    private static final long serialVersionUID = 2L;

    @Name("kafka_topic")
    @Description("this is kafka topic list")
    private String topics = "test1";

    @Name("kafka_broker")
    @Description("this is kafka broker list")
    private String brokers = "localhost:9092";

    @Name("kafka_group_id")
    @Description("this is kafka_group_id")
    private String groupid = "sylph_streamSql_test1";

    @Name("auto.offset.reset")
    @Description("this is auto.offset.reset mode")
    private String offsetMode = "latest";

    @Name("reset.timestamp")
    @Description("this is auto.offset.reset mode")
    private String timestamp = DateUtils.format(new Date(),DateUtils.SECOND);

    @Name("reset.partitions")
    @Description("this is auto.offset.reset mode")
    private String partitions ;

    @Name("zookeeper.connect")
    @Description("this is kafka zk list, kafka08 and kafka09 Must need to set")
    private String zookeeper = null;   //"localhost:2181"

    @Name("value_type")
    @Description("this is kafka String value Type, use json")
    private String valueType;
    @Name("krbs.on")
    @Description("this is kafka String value Type, use json")
    private Boolean isKrbs = false;

    /**
     * canal kafka mutlti lines
     */
    @Name("filter_table")
    @Description("this is kafka filter table name, use MultiSource")
    private String filterTableName = "";

    @Name("filter_ddl")
    @Description("this is kafka filter DDL, use MultiSource")
    private Boolean filterDDL = true;

    @Name("files")
    @Description("this is kafka filter fields, use MultiSource")
    private String files = "";

    public String getTimestamp() {
        return timestamp;
    }

    public String getPartitions() {
        return partitions;
    }

    public Boolean getFilterDDL() {
        return filterDDL;
    }

    public String getFiles() {
        return files;
    }

    public String getFilterTableName() {
        return filterTableName;
    }

    public String getTopics() {
        return topics;
    }

    public String getBrokers() {
        return brokers;
    }

    public String getGroupid() {
        return groupid;
    }

    public String getOffsetMode() {
        return offsetMode;
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public String getValueType() {
        return valueType;
    }

    public Boolean getKrbs() {
        return isKrbs;
    }

    private KafkaSourceConfig() {}
}
