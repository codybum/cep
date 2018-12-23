package io.cresco.cep;

import io.cresco.cep.engine.ESPEREngine;
import io.cresco.cep.engine.QueryEntry;
import io.cresco.cep.engine.Ticker;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.util.UUID;

public class Launcher {

	
	
	public static ESPEREngine ep;



	
	public static void main(String[] args) throws Exception 
	{

		//ep = new ESPEREngine();

        //ep.addEvent();

        String query = "select * from CarLocUpdateEvent";
        String queryid = UUID.randomUUID().toString();
		String query_name = "myQuery";
		String query_schema = ReflectData.get().getSchema(Ticker.class).toString();
		String eventTypeName = "CarLocUpdateEvent";
        String timestamp = String.valueOf(System.currentTimeMillis());

        QueryEntry qe = new QueryEntry(queryid,query_name,query,query_schema,eventTypeName,timestamp);
        ep.addQuery(qe);

        String query2 = "select source from CarLocUpdateEvent where value > 1";
        String queryid2 = UUID.randomUUID().toString();
        String query_name2 = "myQuery2";
        String query_schema2 = ReflectData.get().getSchema(Ticker.class).toString();
        String eventTypeName2 = "CarLocUpdateEvent";
        String timestamp2 = String.valueOf(System.currentTimeMillis());


        QueryEntry q2 = new QueryEntry(queryid2,query_name2,query2,query_schema2,eventTypeName2,timestamp2);
        ep.addQuery(q2);

        //ep.addQuery("1","@EventRepresentation(avro) select source from CarLocUpdateEvent where value > 1","CarLocUpdateEvent");

        String inputStr = "al2s.internet2.edu,urn:publicid:IDN+al2s.internet2.edu+interface+sdn-sw.colu4.net.internet2.edu:eth1/1,rx_pps,1433967060000,259012";
        String[] sstr = inputStr.split(",");

        //Create Instance
        String source = sstr[0];
        String urn = sstr[1];
        String metric = sstr[2];
        long ts = Long.parseLong(sstr[3]);
        double value = Double.parseDouble(sstr[4]);

        Ticker tick = new Ticker(source,urn,metric,ts,value);

        //Instance to GenericData.Record
        Schema schema = ReflectData.get().getSchema(Ticker.class);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Encoder encoder = new EncoderFactory().jsonEncoder(schema, outputStream);
        DatumWriter<Ticker> writer = new ReflectDatumWriter<>(schema);
        writer.write(tick, encoder);
        encoder.flush();
        String input = new String(outputStream.toByteArray());



        ep.input(input,eventTypeName);
        Thread.sleep(1000);
        ep.input(input,eventTypeName);
        Thread.sleep(1000);
        ep.input(input,eventTypeName);
        Thread.sleep(1000);
        ep.input(input,eventTypeName);
        Thread.sleep(1000);

        ep.delQuery(queryid2);

        ep.input(input,eventTypeName);
        Thread.sleep(1000);
        ep.input(input,eventTypeName);
        Thread.sleep(1000);
        ep.input(input,eventTypeName);
        Thread.sleep(1000);
        ep.input(input,eventTypeName);
        Thread.sleep(1000);


    }

}
