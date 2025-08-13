package com.yourcompany.avroexample;

import com.yourcompany.generated.PassengerNotification;
import java.io.File;

public class Main {
    public static void main(String[] args) throws Exception {
        // 1. Unmarshal XML to POJO
        PassengerNotification pojo = AvroUtil.fromXml(
            new File("src/main/resources/FLIGHT_CANCEL_BUS_SELECT_BOOKING_CLASS_K_Input.xml"),
            PassengerNotification.class
        );

        // 2. Load Avro schema
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(
            new File("src/main/resources/PassengerNotification.avsc")
        );

        // 3. Map POJO to Avro GenericRecord
        org.apache.avro.generic.GenericRecord record = AvroMapper.mapToAvro(pojo, schema);

        // 4. Serialize Avro record to JSON
        String json = AvroUtil.avroToJson(record, schema);

        // 5. Output
        System.out.println(json);
    }
}