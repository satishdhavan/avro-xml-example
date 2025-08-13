package com.yourcompany.avroexample;

import com.yourcompany.generated.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("unchecked")
public class AvroMapper {
    public static GenericRecord mapToAvro(PassengerNotification pojo, Schema schema) {
        GenericRecord rec = new GenericData.Record(schema);

        // Example mapping: processingData
        if (pojo.getProcessingData() != null) {
            rec.put("processingData", mapProcessingData(pojo.getProcessingData(), schema.getField("processingData").schema()));
        } else {
            rec.put("processingData", null);
        }

        // Example mapping: recordLocator (list)
        List<RecordLocator> rlList = pojo.getRecordLocator();
        if (rlList != null) {
            List<GenericRecord> avroList = new ArrayList<>();
            Schema rlSchema = schema.getField("recordLocator").schema().getElementType();
            for (RecordLocator rl : rlList) {
                GenericRecord rlRec = new GenericData.Record(rlSchema);
                rlRec.put("recordNumber", rl.getRecordNumber());
                rlRec.put("recordType", rl.getRecordType());
                avroList.add(rlRec);
            }
            rec.put("recordLocator", avroList);
        } else {
            rec.put("recordLocator", new ArrayList<>());
        }

        // Expand this pattern for remaining fields
        // ... (see detailed mapping example above for full method patterns)

        return rec;
    }

    private static GenericRecord mapProcessingData(ProcessingData pd, Schema pdSchemaUnion) {
        Schema pdSchema = extractRecordSchema(pdSchemaUnion);
        GenericRecord rec = new GenericData.Record(pdSchema);

        if (pd.getProcessingDateTime() != null) {
            rec.put("processingDateTime", mapDateTime(pd.getProcessingDateTime(), pdSchema.getField("processingDateTime").schema()));
        } else {
            rec.put("processingDateTime", null);
        }
        rec.put("responsibleOffice", pd.getResponsibleOffice());
        // ... expand for all fields as needed
        return rec;
    }

    private static GenericRecord mapDateTime(Object dt, Schema dtSchemaUnion) {
        Schema dtSchema = extractRecordSchema(dtSchemaUnion);
        GenericRecord rec = new GenericData.Record(dtSchema);
        // Assume dt has getDate() and getTime()
        try {
            rec.put("date", (String)dt.getClass().getMethod("getDate").invoke(dt));
            rec.put("time", (String)dt.getClass().getMethod("getTime").invoke(dt));
        } catch (Exception ignored) {}
        return rec;
    }

    private static Schema extractRecordSchema(Schema unionSchema) {
        if (unionSchema.getType() == Schema.Type.UNION) {
            for (Schema s : unionSchema.getTypes()) {
                if (s.getType() == Schema.Type.RECORD) return s;
            }
        }
        return unionSchema;
    }
}