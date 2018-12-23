package io.cresco.cep.engine;

public class QueryEntry {

	  public String query_id;
	  public String query_name;
	  public String query;
	  public String inputSchema;
	  public String eventTypeName;
	  public long timestamp;

	  public QueryEntry(String query_id, String query_name, String query, String inputSchema, String eventTypeName)
	  {
		  this.query_id = query_id;
		  this.query_name = query_name;
		  this.query = "@EventRepresentation(avro) " + query;
		  //this.query = query;
		  this.inputSchema = inputSchema;
		  this.eventTypeName = eventTypeName;
		  this.timestamp = System.currentTimeMillis();
	  }


	  public QueryEntry(String query_id, String query_name, String query, String inputSchema, String eventTypeName, String timestamp)
	  {
		  this.query_id = query_id;
		  this.query_name = query_name;
          this.query = "@EventRepresentation(avro) " + query;
		  //this.query =  query;
		  this.inputSchema = inputSchema;
		  this.eventTypeName = eventTypeName;
		  this.timestamp = Long.parseLong(timestamp);
	  }
	    	  
	}