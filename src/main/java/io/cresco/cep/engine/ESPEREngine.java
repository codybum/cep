package io.cresco.cep.engine;

import com.espertech.esper.client.*;
import com.espertech.esper.client.util.ClassForNameProvider;
import com.espertech.esper.event.avro.AvroSchemaEventType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


public class ESPEREngine {
 
	private Map<String,CEPListener> listeners;
	private Map<String,EPStatement> statements;
    private Map<String,Schema> schemas;

    private AtomicBoolean lockListeners = new AtomicBoolean();
    private AtomicBoolean lockStatements = new AtomicBoolean();
    private AtomicBoolean lockSchemas = new AtomicBoolean();
    private AtomicBoolean lockIsActive = new AtomicBoolean();

    public EPRuntime cepRT;
	public EPAdministrator cepAdm;

    private PluginBuilder plugin;
    private CLogger logger;

    private boolean isActive = false;

    public ESPEREngine(PluginBuilder plugin)
	{
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);

        listeners = Collections.synchronizedMap(new HashMap<>());
		statements = Collections.synchronizedMap(new HashMap<>());
		schemas = Collections.synchronizedMap(new HashMap<>());
	}

	public void initEngine() {

	    try {

            //The Configuration is meant only as an initialization-time object.
            //configure cache size
            ConfigurationDBRef configDB = new ConfigurationDBRef();
            configDB.setLRUCache(10);

            Configuration cepConfig = new Configuration();
            cepConfig.addDatabaseReference("MyDB", configDB);

            cepConfig.getEngineDefaults().getEventMeta().getAvroSettings().setEnableAvro(true);
            cepConfig.getEngineDefaults().getEventMeta().getAvroSettings().setEnableNativeString(false);
            cepConfig.getEngineDefaults().getEventMeta().getAvroSettings().setEnableSchemaDefaultNonNull(true);
            cepConfig.getEngineDefaults().getEventMeta().getAvroSettings().setObjectValueTypeWidenerFactoryClass(null);
            cepConfig.getEngineDefaults().getEventMeta().getAvroSettings().setTypeRepresentationMapperClass(null);

            EPServiceProvider cep = EPServiceProviderManager.getProvider("myCEPEngine", cepConfig);

            cep.getEPAdministrator().getConfiguration().getTransientConfiguration().put(ClassForNameProvider.NAME,
                    new ClassForNameProvider() {
                        public Class classForName(String className) throws ClassNotFoundException {
                            //not sure why this hackish thing is needed, but it is for some OSGi reason.
                            if(className.equals("EventRepresentation")) {
                                ClassLoader original = Thread.currentThread().getContextClassLoader();
                                //System.out.println("ENTERING com.espertech.esper.client.annotation.EventRepresentation");
                                ClassLoader cl = com.espertech.esper.client.annotation.EventRepresentation.class.getClassLoader();
                                Thread.currentThread().setContextClassLoader(cl);
                                Class requestedClass = Class.forName(className, true, cl);
                                Thread.currentThread().setContextClassLoader(original);
                                return requestedClass;
                            }

                            //for some other reason, we can't use the default ESPER class loader
                            //return Class.forName(className, true, Thread.currentThread().getContextClassLoader());
                            return Class.forName(className);
                        }
                    });


            cep.initialize();
            cepRT = cep.getEPRuntime();
            cepAdm = cep.getEPAdministrator();

            synchronized (lockIsActive) {
                isActive = true;
            }

        } catch(Exception ex) {
	        logger.error("initEngine() " + ex.getMessage());
        }

    }

	public class CEPListener implements UpdateListener {
    	public String query_id;
    	public CEPListener(String query_id)
    	{
    		this.query_id = query_id;
    	}
    	public void update(EventBean[] newEvents, EventBean[] oldEvents) {
    		if (newEvents != null)
            {
                    for(EventBean eventBean : newEvents) {

                        try {
                            System.out.println("query_id=" + query_id + " OUTPUT=" + (GenericData.Record) eventBean.getUnderlying());
                        } catch(Exception ex) {
                            ex.printStackTrace();
                        }

            		}
            }
            if (oldEvents != null) 
            {
            	 System.out.println("Old Event received: " + oldEvents[0].getUnderlying());
            }
        }
    }

    public boolean delQuery(String query_id) {
    	try
    	{
            EPStatement cepStatement = null;
            synchronized (lockStatements) {
                cepStatement = statements.get(query_id);
            }
            synchronized (lockListeners) {
                CEPListener c = listeners.get(query_id);
                cepStatement.removeListener(c);
                c = null;
                cepStatement.stop();
                cepStatement.destroy();
                cepStatement = null;
                listeners.remove(query_id);
                System.out.println("query_id=" + query_id + " REMOVED LISTENER");
            }
            synchronized (lockStatements) {
                statements.remove(query_id);
                System.out.println("query_id=" + query_id + " REMOVED STATEMENT");
            }
    		return true;
    	}
    	catch(Exception ex)
    	{
    		System.out.println("ESPEREngine delQuery: " + ex.toString());
    		ex.printStackTrace();
    		return false;
    	}
    	
    }

	private Schema getOutPutSchema(String query_id) {
	    Schema outputSchema = null;
	    try {

	        if(statements.containsKey(query_id)) {
	            outputSchema = (Schema) ((AvroSchemaEventType)statements.get(query_id).getEventType()).getSchema();
            }

        } catch(Exception ex) {
	        ex.printStackTrace();
        }
	    return outputSchema;
    }

    public String getStatementSchema(String queryId) {

        String schemaString = null;
        try {
            synchronized (lockStatements) {
                if(statements.containsKey(queryId)) {
                    Schema schema = (Schema) ((AvroSchemaEventType) statements.get(queryId).getEventType()).getSchema();
                    schemaString = schema.toString();
                }
            }
        } catch(Exception ex) {
            logger.error("getStatementSchema(): " + ex.getMessage());
        }
        return schemaString;
    }

    public boolean addQuery(QueryEntry qe) {

        synchronized (lockIsActive) {
            if(!isActive) {
                initEngine();
            }
        }

        if(!isActive) {
            return false;
        } else {
            try {

                /*
                String query = "select * from CarLocUpdateEvent";
                String queryid = UUID.randomUUID().toString();
                String query_name = "myQuery";
                String query_schema = ReflectData.get().getSchema(Ticker.class).toString();
                String eventTypeName = "CarLocUpdateEvent";
                String timestamp = String.valueOf(System.currentTimeMillis());

                qe = new QueryEntry(queryid,query_name,query,query_schema,eventTypeName,timestamp);
                */

                boolean hasEvent = false;
                if (cepAdm.getConfiguration().getEventType(qe.eventTypeName) == null) {
                    logger.info("query_id=" + qe.query_id + " eventType=" + qe.eventTypeName + " MISSING");
                    Schema.Parser p = new Schema.Parser();
                    Schema schema = p.parse(qe.inputSchema);
                    if (schema != null) {
                        ConfigurationEventTypeAvro avroEvent = new ConfigurationEventTypeAvro(schema);
                        logger.info("query_id=" + qe.query_id + " eventType=" + qe.eventTypeName + " SCHEMA CREATED");

                        try {
                            cepAdm.getConfiguration().addEventTypeAvro("CarLocUpdateEvent", avroEvent);
                        } catch(Exception ex) {
                            StringWriter errors = new StringWriter();
                            ex.printStackTrace(new PrintWriter(errors));
                            logger.error("T1 Failed: " + errors.toString());
                        }

                        synchronized (lockSchemas) {
                            schemas.put(qe.eventTypeName, schema);
                        }
                        System.out.println("query_id=" + qe.query_id + " eventType=" + qe.eventTypeName + " ADDED");
                        hasEvent = true;
                    } else {
                        System.out.println("SCHEMA == null");
                    }
                } else {
                    System.out.println("query_id=" + qe.query_id + " eventType=" + qe.eventTypeName + " EXISTING RECORD FOUND");
                    hasEvent = true;
                }

                if (hasEvent) {
                    EPStatement cepStatement = cepAdm.createEPL(qe.query);

                    CEPListener c = new CEPListener(qe.query_id);
                    cepStatement.addListener(c);

                    synchronized (lockListeners) {
                        listeners.put(qe.query_id, c);
                        System.out.println("query_id=" + qe.query_id + " ADDED LISTENER");
                    }
                    synchronized (lockStatements) {
                        statements.put(qe.query_id, cepStatement);
                        System.out.println("query_id=" + qe.query_id + " ADDED STATEMENT");
                    }
                }
                return true;
            } catch (Exception ex) {
                System.out.println("ESPEREngine addQuery: " + ex.toString());
                ex.printStackTrace();
                return false;
            }
        }
    }

    public void input(String inputStr, String eventTypeName) {

        if(cepRT == null) {
            logger.error("Input Data with no CEP Engine init");
        } else {
            try {

                if (schemas.containsKey(eventTypeName)) {
                    Schema schema = schemas.get(eventTypeName);
                    Decoder decoder = new DecoderFactory().jsonDecoder(schema, inputStr);
                    DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
                    GenericData.Record record = reader.read(null, decoder);
                    System.out.println("INCOMING DATA FOR EVENTTYPE " + eventTypeName);
                    cepRT.sendEventAvro(record, eventTypeName);

                } else {
                    System.out.println("NO SCHEMA FOR INCOMING DATA FOR EVENTTYPE " + eventTypeName);
                }

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private void input2(String inputStr) throws ParseException {
    	//al2s.internet2.edu,urn:publicid:IDN+al2s.internet2.edu+interface+sdn-sw.colu4.net.internet2.edu:eth1/1,rx_pps,1433967060000,259012
        //al2s.internet2.edu,urn:publicid:IDN+al2s.internet2.edu+interface+sdn-sw.elpa.net.internet2.edu:xe-5/0/3.0,rx_pps,1490710820000,106538
        try
    	{
    		String[] sstr = inputStr.split(",");
    		if(sstr.length ==5)
    		{
    			String source = sstr[0];
    			String urn = sstr[1];
    			String metric = sstr[2];
    			long ts = Long.parseLong(sstr[3]);
    			double value = Double.parseDouble(sstr[4]);
    	
    			Ticker tick = new Ticker(source,urn,metric,ts,value);

				Schema schema = ReflectData.get().getSchema(Ticker.class);
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				Encoder encoder = new EncoderFactory().jsonEncoder(schema, outputStream);
				DatumWriter<Ticker> writer = new ReflectDatumWriter<>(schema);
				writer.write(tick, encoder);
				encoder.flush();


				String input = new String(outputStream.toByteArray());
				System.out.println("Original Object Schema JSON: " + schema);
				System.out.println("Original Object DATA JSON: "+ input);


				Decoder decoder = new DecoderFactory().jsonDecoder(schema, input);
				DatumReader<GenericData.Record> reader = new GenericDatumReader<>(schema);
				GenericData.Record logs = reader.read(null, decoder);
				System.out.println("Transmitted Object DATA JSON: " + logs);


				cepRT.sendEventAvro(logs,"CarLocUpdateEvent");

    		}


    	}
    	catch(Exception ex)
    	{
    		System.out.println("ESPEREngine : Input Tick : " + ex.toString());
    		System.out.println("ESPEREngine : Input Tick : InputStr " + inputStr);
    		ex.printStackTrace();
    	}
    	
    }

}