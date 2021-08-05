package com.github.spadariya.kafka.connect.smt;


import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ExtractUserid<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Insert a random UUID into a connect record";
    private static final Logger logger = LoggerFactory.getLogger(ExtractUserid.class);

    private interface ConfigName {
        String EXTRACT_FROM = "extract.from.field";
        String EXTRACT_TO = "extract.to.field";
        String EXTRACT_REGEX = "extract.regex.pattern";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.EXTRACT_FROM, ConfigDef.Type.STRING, "message", ConfigDef.Importance.HIGH,
                    "Extract user id from this field")
            .define(ConfigName.EXTRACT_TO, ConfigDef.Type.STRING, "user_id", ConfigDef.Importance.HIGH,
                    "Extract user id to this field")
            .define(ConfigName.EXTRACT_REGEX, ConfigDef.Type.STRING, "no_pattern_given", ConfigDef.Importance.HIGH,
                    "Extract numeric vause using this regex");

    private static final String PURPOSE = "Extract first numeric value from string";

    private String extractFromfieldName;
    private String extractTofieldName;
    private String extractRegexPattern;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        extractFromfieldName = config.getString(ConfigName.EXTRACT_FROM);
        extractTofieldName = config.getString(ConfigName.EXTRACT_TO);
        extractRegexPattern = config.getString(ConfigName.EXTRACT_REGEX);

        logger.info("------------- Configure Parameters -------------");
        logger.info("extractFromfieldName : "+extractFromfieldName);
        logger.info("extractTofieldName : "+extractTofieldName);
        logger.info("extractRegexPattern : "+extractRegexPattern);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }


    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);

        updatedValue.put(extractTofieldName, getUserid(value.get(extractFromfieldName)));

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if(updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        updatedValue.put(extractTofieldName, getUserid(value.get(extractFromfieldName)));

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    private long getUserid(Object message) {
        String messageValue = String.valueOf(message);

        //get first number from string
        Pattern regexPattern = Pattern.compile(extractRegexPattern);
        Matcher matcher = regexPattern.matcher(messageValue);

        Long user_id = 130l;
        if(matcher.find()) {
            user_id = Long.parseLong(matcher.group(1));
        }

        return user_id;
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field: schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(extractTofieldName, Schema.INT64_SCHEMA);
        return builder.build();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ExtractUserid<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends ExtractUserid<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }
}


