/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.xjd.bd.commons;

/**
 * Created by XJD on 2016/10/14.
 */

import com.xjd.bd.entity.MyRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;

public class JsonToAvro {
    private static final String monitorJson = "{\"spt\":62725,\"typ\":1,\"dip\":\"192.168.101.57\",\"prt\":175,\"sip\":\"192.168.101.66\",\"dpt\":21,\"pln\":null,\"ctm\":\"2017-05-04 17:19:40\",\"atr\":{\"mtm\":null,\"url\":null,\"usr\":null,\"mfr\":null,\"eml\":null,\"mto\":null,\"sbj\":\"2017-05-04 17:19:40\",\"mid\":null},\"flt\":[{\"cue\":\"新建文本文档.txt\",\"nty\":0,\"pid\":1,\"suc\":1,\"zip\":0,\"lev\":0,\"typ\":\"txt\",\"txt\":\"/opt/TM_DLP/tempfile/session_stream/080241ab-78cf-49d4-b406-69c33dd6b9c2.txt\",\"fsz\":148,\"ecr\":0,\"fnm\":\"新建文本文档.txt\",\"aut\":\"unknow\",\"pth\":\"/opt/TM_DLP/tempfile/session_stream/192.168.101.66_62727_192.168.101.57_11469.txt\",\"fid\":1}],\"pid\":26,\"pvr\":7,\"gid\":122,\"o_ret\":{\"ret_f_array\":[0],\"ret_sbj\":0,\"ret_pln\":0},\"lvl\":1,\"snp\":[{\"cid\":466,\"cty\":1,\"tgs\":[{\"tgt\":3,\"mts\":1,\"dat\":{\"fid\":0,\"pid\":1,\"tms\":11,\"wds\":1,\"cue\":\"新建文本文档.txt\",\"scs\":[{\"cut\":\"信息安全事件\",\"wos\":0,\"wln\":18,\"tos\":0},{\"cut\":\"\\r\\n信息安全事件\",\"wos\":2,\"wln\":18,\"tos\":18},{\"cut\":\" 信息安全事件\",\"wos\":1,\"wln\":18,\"tos\":38},{\"cut\":\" 信息安全事件\",\"wos\":1,\"wln\":18,\"tos\":57},{\"cut\":\" 信息安全事件\",\"wos\":1,\"wln\":18,\"tos\":76},{\"cut\":\" 信息安全事件\",\"wos\":1,\"wln\":18,\"tos\":95},{\"cut\":\"\\r\\n信息安全事件\",\"wos\":2,\"wln\":18,\"tos\":114},{\"cut\":\"\\r\\n信息安全事件\",\"wos\":2,\"wln\":18,\"tos\":134},{\"cut\":\"\\r\\n信息安全事件\",\"wos\":2,\"wln\":18,\"tos\":154},{\"cut\":\"\\r\\n信息安全事件\",\"wos\":2,\"wln\":18,\"tos\":174},{\"cut\":\"\\r\\n信息安全事件\",\"wos\":2,\"wln\":18,\"tos\":194}]}}]}],\"rls\":[227],\"ept\":0}";
    private static final Logger logger = LoggerFactory.getLogger(JsonToAvro.class);
    public JsonToAvro(){

    }
    public byte[] jsonToAvro(String json, Schema schema) throws IOException {
        InputStream input = null;
        GenericDatumWriter<GenericRecord> writer;
        Encoder encoder;
        ByteArrayOutputStream output;
        try {
            //Schema schema = new Schema.Parser().parse(schemaStr);
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            input = new ByteArrayInputStream(json.getBytes());
            output = new ByteArrayOutputStream();
            DataInputStream din = new DataInputStream(input);
            writer = new GenericDatumWriter<GenericRecord>(schema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            encoder = EncoderFactory.get().binaryEncoder(output, null);
            GenericRecord datum;
            while (true) {
                try {
                    datum = reader.read(null, decoder);
                } catch (EOFException eofe) {
                    break;
                }
                writer.write(datum, encoder);
            }
            encoder.flush();
            return output.toByteArray();
        } finally {
            try {
                assert input != null;
                input.close(); } catch (Exception ignored) { }
        }
    }

    public  String avroToJson(byte[] avro, Schema schema) throws IOException {
        GenericDatumReader<GenericRecord> reader = null;
        JsonEncoder encoder;
        ByteArrayOutputStream output = null;
        try {
            //Schema schema = new Schema.Parser().parse(schemaStr);
            reader = new GenericDatumReader<>(schema);
            InputStream input = new ByteArrayInputStream(avro);
            output = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
            encoder = EncoderFactory.get().jsonEncoder(schema, output, false);
            Decoder decoder = DecoderFactory.get().binaryDecoder(input, null);
            GenericRecord datum;
            while (true) {
                try {
                    datum = reader.read(null, decoder);
                } catch (EOFException eofe) {
                    break;
                }
                writer.write(datum, encoder);
            }
            encoder.flush();
            output.flush();
            return new String(output.toByteArray());
        } finally {
            try {
                if (output != null) output.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String avroToJsonByClass(byte[] avro, Schema schema) throws IOException {

        SpecificDatumReader<MyRecord> reader = new SpecificDatumReader<MyRecord>(MyRecord.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(avro, null);
        MyRecord myRecord = reader.read(null, decoder);
        System.out.println(myRecord);
        return null;
    }

    public void myDeserialize(byte[] avro, Schema schema) throws IOException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(avro);
        Decoder decoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        GenericRecord genericRecord  = datumReader.read(null, decoder);
        Object[] keyParts = new Object[genericRecord.getSchema().getFields().size()];
        for (int i = 0; i < genericRecord.getSchema().getFields().size(); i++) {
            keyParts[i] = genericRecord.get(i);
            String str = schema.getFields().get(i).toString().split(" ")[0];
            //System.out.println(genericRecord.getSchema());

            System.out.println(str + " : " + keyParts[i] + ":" + genericRecord.get(str));
        }
    }

    public Schema getSchema(String schemaFile) throws IOException {
        Schema schema;
        try{
            File file = new File(schemaFile);
            schema = new Schema.Parser().parse(file);
            return schema;
        }catch (IOException e){
            logger.error("parse schema error" + e.toString());
        }
        return null;
    }
    public static void main(String[] args){
        JsonToAvro jsonToAvro = new JsonToAvro();
        Schema schema;
        try{
            String schemaFile = "fileParseJson.avsc";
            schema = jsonToAvro.getSchema(schemaFile);
            System.out.println(Arrays.toString(jsonToAvro.jsonToAvro(monitorJson, schema)));
            //System.out.println(jsonToAvro.avroToJson(jsonToAvro.jsonToAvro(fileParseJson,schema),schema));
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
