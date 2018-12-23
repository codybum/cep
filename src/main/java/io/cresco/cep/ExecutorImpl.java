package io.cresco.cep;

import com.google.gson.Gson;
import com.sun.org.apache.xpath.internal.operations.Bool;
import io.cresco.cep.engine.ESPEREngine;
import io.cresco.cep.engine.QueryEntry;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

public class ExecutorImpl implements Executor {

    private PluginBuilder plugin;
    private CLogger logger;
    private ESPEREngine ep;
    private Gson gson;

    public ExecutorImpl(PluginBuilder pluginBuilder, ESPEREngine ep) {
        this.plugin = pluginBuilder;
        logger = plugin.getLogger(ExecutorImpl.class.getName(),CLogger.Level.Info);
        this.ep = ep;
        this.gson = new Gson();
    }

    @Override
    public MsgEvent executeCONFIG(MsgEvent incoming) {

        if(incoming.getParam("action") != null) {

            switch (incoming.getParam("action")) {
                case "addquery":
                    return addCEPQuery(incoming);

                default:
                    logger.error("Unknown configtype found: {} {}", incoming.getParam("action"), incoming.getMsgType());
                    return null;
            }
        }
        return null;

    }
    @Override
    public MsgEvent executeDISCOVER(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeERROR(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeINFO(MsgEvent incoming) {
        logger.info("INCOMING INFO MESSAGE : " + incoming.getParams());
        //System.out.println("INCOMING INFO MESSAGE FOR PLUGIN");
        return null;
    }

    @Override
    public MsgEvent executeEXEC(MsgEvent incoming) {

        //logger.info("INCOMING EXEC MESSAGE : " + incoming.getParams());

        if(incoming.getParam("action") != null) {

            switch (incoming.getParam("action")) {
                case "queryinput":
                    queryinput(incoming);
                    break;

                default:
                    logger.error("Unknown configtype found: {} {}", incoming.getParam("action"), incoming.getMsgType());
                    return null;
            }
        }
        return null;

    }
    @Override
    public MsgEvent executeWATCHDOG(MsgEvent incoming) {
        return null;
    }
    @Override
    public MsgEvent executeKPI(MsgEvent incoming) {
        return null;
    }


    public MsgEvent addCEPQuery(MsgEvent incoming) {
        if(incoming.getParam("query") != null) {

            String queryObjString = incoming.getParam("query");
            QueryEntry qe = gson.fromJson(queryObjString,QueryEntry.class);
            logger.info("INCOMING QUERY ADD: " + incoming.getParams().toString());
            if(ep.addQuery(qe)) {
                String outputSchemaString = ep.getStatementSchema(qe.query_id);
                if(outputSchemaString != null) {
                    incoming.setCompressedParam("outputschema", outputSchemaString);
                    incoming.setParam("addquery", Boolean.TRUE.toString());
                } else {
                    incoming.setParam("addquery", Boolean.FALSE.toString());
                }

            } else {
                incoming.setParam("addquery", Boolean.FALSE.toString());
            }
        }

        return incoming;
    }

    public void queryinput(MsgEvent incoming) {

        String input = incoming.getParam("input");
        String eventtypename = incoming.getParam("eventtypename");
        //logger.info("SENDING ESPER INPUT!");
        ep.input(input,eventtypename);

    }

}