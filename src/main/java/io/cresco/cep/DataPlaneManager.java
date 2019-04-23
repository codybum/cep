package io.cresco.cep;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cresco.cep.engine.QueryEntry;
import io.cresco.cep.engine.Ticker;
import io.cresco.library.app.gEdge;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DataPlaneManager {

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;
    private List<gEdge> edgeList;
    private String myInodeId;
    private String myResourceId;
    private boolean isInitQuery = false;

    private Map<String,Map<String,String>> inodeMap = null;


    public DataPlaneManager(PluginBuilder plugin, String edges) {
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        gson = new Gson();

        if(edges != null) {
            Type type = new TypeToken<List<gEdge>>(){}.getType();
            edgeList = gson.fromJson(edges,type);

        }

        inodeMap = new HashMap<>();

        myInodeId = plugin.getConfig().getStringParam("inode_id");
        myResourceId = plugin.getConfig().getStringParam("resource_id");

    }

    public boolean initQuery(String regionId, String agentId, String pluginId) {

        boolean isAdded = false;
        try {

            //String query = "select * from CarLocUpdateEvent";
            String query = "select source from CarLocUpdateEvent where value > 1000";
            String queryid = UUID.randomUUID().toString();
            String query_name = "myQuery";
            String query_schema = ReflectData.get().getSchema(Ticker.class).toString();
            String eventTypeName = "CarLocUpdateEvent";
            String timestamp = String.valueOf(System.currentTimeMillis());

            QueryEntry qe = new QueryEntry(queryid,query_name,query,query_schema,eventTypeName,timestamp);

            String action_addquery = gson.toJson(qe);

            MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG, regionId, agentId, pluginId);
            request.setParam("action","addquery");
            request.setParam("query",action_addquery);

            MsgEvent returnMsg = plugin.sendRPC(request);
            if(returnMsg.getParam("addquery") != null) {
                if(returnMsg.getParam("addquery").equals("true")){
                    isAdded = true;
                }
            }

        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }


        return isAdded;
    }

    public boolean sendInput(String regionId, String agentId, String pluginId) {

        boolean isSent = false;
        try {

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

            MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, regionId, agentId, pluginId);
            request.setParam("action","queryinput");
            request.setParam("eventtypename","CarLocUpdateEvent");
            request.setParam("input",input);
            plugin.msgOut(request);
            isSent = true;

        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }

        return isSent;
    }

    private Map<String,String> getPNode(String inodeId) {
        Map<String,String> pNodeMap = null;
        try {

            MsgEvent request = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.EXEC);
            request.setParam("action", "getinodestatus");
            request.setParam("inode_id",inodeId);
            request.setParam("resource_id", myResourceId);

            MsgEvent ce = plugin.sendRPC(request);

            Type type = new TypeToken<Map<String, String>>(){}.getType();
            pNodeMap = gson.fromJson(ce.getCompressedParam("pnode"), type);

            logger.info("payload: " + ce.getParams().toString());
            logger.info("pnode: " + pNodeMap.toString());

        } catch(Exception ex) {
            logger.error(ex.getMessage());
        }
        return pNodeMap;
    }

    private int getNodeStatus(String inodeId) {
        int status = -1;
        try {

            MsgEvent request = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.EXEC);
            request.setParam("action", "getinodestatus");
            request.setParam("inode_id", inodeId);
            request.setParam("resource_id", myResourceId);

            MsgEvent response = plugin.sendRPC(request);
            status = Integer.parseInt(response.getParam("status_code"));

            if(status == 10) {
                if (!inodeMap.containsKey(inodeId)) {
                    Type type = new TypeToken<Map<String, String>>() {
                    }.getType();
                    inodeMap.put(inodeId, gson.fromJson(response.getCompressedParam("pnode"), type));
                }
            }

        } catch (Exception ex) {
            logger.error(ex.getMessage());
        }
        return status;
    }

    public void forward() {

        if(plugin.isActive()) {
            try {


                if((edgeList != null) && (myInodeId != null)) {
                    for(gEdge edge : edgeList) {
                        //logger.info("edge_id: " + edge.edge_id);
                        if(edge.node_from.equals(myInodeId)) {
                            //logger.info("(" + myInodeId + ") SENDING TO " + edge.node_to);

                            try {

                                if(inodeMap.containsKey(edge.node_to)) {

                                    String remoteAgent = inodeMap.get(edge.node_to).get("agent");
                                    String remoteRegion = inodeMap.get(edge.node_to).get("region");
                                    String remotePlugin = inodeMap.get(edge.node_to).get("agentcontroller");

                                    if (!isInitQuery) {
                                        logger.info("Init remote query");
                                        isInitQuery = initQuery(remoteRegion, remoteAgent, remotePlugin);

                                        if (!isInitQuery) {
                                            logger.error("query init failed");
                                        }

                                    }

                                    if (isInitQuery) {
                                        //logger.info("SEND INPUT!");
                                        if (!sendInput(remoteRegion, remoteAgent, remotePlugin)) {
                                            logger.error("Error sending message");
                                        }
                                    }

                                } else {
                                    int status = getNodeStatus(edge.node_to);
                                    logger.info("inode: " + edge.node_to + " status:" + status);
                                }

                            } catch(Exception ex) {
                                logger.error(ex.getMessage());
                            }
                        }

                        if(edge.node_to.equals(myInodeId)) {
                            //logger.info("(" + myInodeId + ") RECEVING FROM " + edge.node_from);
                        }

                    }
                }
            } catch(Exception ex) {
                logger.error("Forward() : " + ex.getMessage());
            }
        }
    }


}
