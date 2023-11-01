package com.parquet.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;
@RestController
@RequestMapping("/api")
public class ParquetController {

	   @PostMapping("/saveToParquet")
	    public ResponseEntity<String> saveToParquet(@RequestBody Map<String, Object> jsonData) {
	        try {
	            // Infer the Avro schema dynamically based on the JSON structure
	            Schema schema = generateAvroSchema(jsonData);

	            // Create a ParquetWriter
	            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
	                            new org.apache.hadoop.fs.Path("C:\\Users\\user\\Downloads\\HD\\example.parquet"))
	                    .withSchema(schema)
	                    .withCompressionCodec(CompressionCodecName.SNAPPY)
	                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
	                    .withConf(new Configuration())
	                    .build();

	            // Create and write the Avro record
	            GenericRecord record = createAvroRecord(schema, jsonData);
	            writer.write(record);

	            // Close the writer to finish writing the Parquet file
	            writer.close();

	            // Return a success response
	            return ResponseEntity.ok("Data saved to Parquet file.");
	        } catch (IOException e) {
	            e.printStackTrace();
	            // Return an error response
	            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error saving data to Parquet.");
	        }
	    }

	    private Schema generateAvroSchema(Map<String, Object> jsonData) {
	        SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder.record("dynamic_schema").fields();

	        for (Map.Entry<String, Object> entry : jsonData.entrySet()) {
	            String fieldName = entry.getKey();
	            Object fieldValue = entry.getValue();
	            Schema fieldSchema = inferAvroFieldType(fieldValue);
	            builder.name(fieldName).type(fieldSchema).noDefault();
	        }

	        return builder.endRecord();
	    }

	    private Schema inferAvroFieldType(Object fieldValue) {
	        if (fieldValue instanceof Map) {
	            return generateAvroSchema((Map<String, Object>) fieldValue);
	        } else if (fieldValue instanceof Integer) {
	            return Schema.create(Schema.Type.INT);
	        } else if (fieldValue instanceof String) {
	            return Schema.create(Schema.Type.STRING);
	        } else if (fieldValue instanceof List) {
	            if (!((List) fieldValue).isEmpty()) {
	                Object firstValue = ((List) fieldValue).get(0);
	                if (firstValue instanceof Integer) {
	                    return Schema.createArray(Schema.create(Schema.Type.INT));
	                } else if (firstValue instanceof String) {
	                    return Schema.createArray(Schema.create(Schema.Type.STRING));
	                }
	            }
	        }
	        // Default to string if the type cannot be determined
	        return Schema.create(Schema.Type.STRING);
	    }

	    private GenericRecord createAvroRecord(Schema schema, Map<String, Object> jsonData) {
	        GenericRecord record = new GenericData.Record(schema);
	        for (Map.Entry<String, Object> entry : jsonData.entrySet()) {
	            String fieldName = entry.getKey();
	            Object fieldValue = entry.getValue();
	            if (fieldValue instanceof Map) {
	                GenericRecord nestedRecord = createAvroRecord(schema.getField(fieldName).schema(), (Map<String, Object>) fieldValue);
	                record.put(fieldName, nestedRecord);
	            } else if (fieldValue instanceof List) {
	                Schema arraySchema = schema.getField(fieldName).schema();
	                List<Object> arrayValues = (List<Object>) fieldValue;
	                record.put(fieldName, arrayValues);
	            } else {
	                record.put(fieldName, fieldValue);
	            }
	        }
	        return record;
	    }
}
