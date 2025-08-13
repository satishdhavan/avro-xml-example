package com.yourcompany.avroexample;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumWriter;

import java.io.ByteArrayOutputStream;

public class AvroUtil {
    public static <T> T fromXml(File xmlFile, Class<T> clazz) throws Exception {
        JAXBContext context = JAXBContext.newInstance(clazz);
        Unmarshaller unmarshaller = context.createUnmarshaller();
        return clazz.cast(unmarshaller.unmarshal(xmlFile));
    }

    public static String avroToJson(GenericRecord record, Schema schema) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
        writer.write(record, encoder);
        encoder.flush();
        return out.toString("UTF-8");
    }
}