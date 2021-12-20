/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.agilelab.darwin.morphline;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import it.agilelab.darwin.manager.AvroSchemaManager;
import it.agilelab.darwin.manager.AvroSchemaManagerFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.stdio.AbstractParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;


public final class ReadDarwinAvroBuilder implements CommandBuilder {

    /**
     * The MIME type identifier that will be filled into output records
     */
    public static final String AVRO_MEMORY_MIME_TYPE = "avro/java+memory";
    public static final String MESSAGE_FIELDS = "messageFields";
    public static final String DARWIN_CONF = "darwinConf";



    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("readDarwinAvro");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new ReadDarwinAvro(this, config, parent, child, context);
    }


    final class ReadDarwinAvro extends AbstractParser {

        private BinaryDecoder binaryDecoder = null;

        private final AvroSchemaManager schemaManager;
        private final List<String> messageFields;

        public ReadDarwinAvro(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);
            Config darwinConfiguration = getConfigs().getConfig(config, DARWIN_CONF);

            if (darwinConfiguration != null && !darwinConfiguration.isEmpty()) {
                this.schemaManager = AvroSchemaManagerFactory
                        .initialize(darwinConfiguration);
            } else {
                throw new IllegalArgumentException("Darwin configuration is empty");
            }

            messageFields = getConfigs()
                    .getStringList(config, MESSAGE_FIELDS, Collections.singletonList(Fields.ATTACHMENT_BODY));
        }

        @Override
        protected boolean doProcess(Record inputRecord) {
            Record template = inputRecord.copy();

            template.put(Fields.ATTACHMENT_MIME_TYPE, ReadDarwinAvroBuilder.AVRO_MEMORY_MIME_TYPE);

            Map<String, Map<String, Object>> avroMap = decodeMessages(template);

            removeAttachments(template);

            Record outputRecord = template.copy();

            avroMap.forEach((k, v) -> {
                LOG.debug("Setting data {} to record", k);
                outputRecord.put(k, v);
                incrementNumRecords();
            });

            return getChild().process(outputRecord);
        }

        @Override
        protected boolean doProcess(Record record, InputStream stream) throws IOException {
            return false;
        }

        private Map<String, Map<String, Object>> decodeMessages(Record morphlineRecord) {
            Map<String, InputStream> avroMap = new HashMap<>();
            messageFields.forEach(f -> {
                Object value = morphlineRecord.getFirstValue(f);
                if (value != null) {
                    avroMap.put(f, new ByteArrayInputStream((byte[]) value));
                }
            });

            messageFields.forEach(morphlineRecord::removeAll);

            return avroMap.entrySet().stream()
                    .map(el -> {
                        LOG.debug("Decoding message for table: {}", el.getKey());
                        return new AbstractMap.SimpleEntry<>(el.getKey(), decodeAvro(el.getValue()));
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        private Map<String, Object> decodeAvro(InputStream in) {
            Decoder decoder = DecoderFactory.get().binaryDecoder(in, binaryDecoder);
            Schema schema = schemaManager.extractSchema(in).right().get();
            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
            Map<String, Object> recordMap = new HashMap<>();
            try {
                GenericRecord record = datumReader.read(null, decoder);
                record.getSchema().getFields().forEach(x -> recordMap.put(x.name(),
                        Optional.ofNullable(record.get(x.name())).map(Object::toString).orElse(null)));
                return recordMap;
            } catch (IOException e) {
                LOG.error("Unable to decode avro message: ", e);
                return recordMap;
            }
        }

    }

}
