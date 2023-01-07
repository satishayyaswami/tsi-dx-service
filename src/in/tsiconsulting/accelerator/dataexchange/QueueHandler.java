package in.tsiconsulting.accelerator.dataexchange;

import in.tsiconsulting.accelerator.framework.DB;
import in.tsiconsulting.accelerator.framework.DBQuery;
import in.tsiconsulting.accelerator.framework.DBResult;
import in.tsiconsulting.accelerator.framework.HttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Types;
import java.util.Iterator;

public class QueueHandler implements ServletContextListener {

    private final java.net.http.HttpClient httpClient = java.net.http.HttpClient.newBuilder()
            .version(java.net.http.HttpClient.Version.HTTP_2)
            .build();

    public void contextInitialized(ServletContextEvent servletContextEvent) {
        try {
            JSONArray queue = null;
            Iterator<JSONObject> queueIt = null;
            JSONObject request = null;
            JSONObject response = new JSONObject();
            JSONArray participants = null;
            JSONArray registry = null;
            long requestId = 0L;
            String participantId = null;
            String serviceId = null;
            String versionNo = null;
            JSONObject service = null;
            JSONObject participant = null;
            String host = null;
            String uri = null;
            JSONObject data = null;

            participants = new NetworkParticipants().getParticipants();
            registry = new ServiceRegistry().listServices();

            do {
                queue = getQueue();
                queueIt = queue.iterator();
                while (queueIt.hasNext()) {
                    request = (JSONObject) queueIt.next();
                    requestId = (Long) request.get("request_id");
                    participantId = (String) request.get("participant_id");
                    serviceId = (String) request.get("service_id");
                    versionNo = (String) request.get("version_no");
                    participant = getParticipant(participants, participantId);
                    service = getService(registry, participantId, serviceId, versionNo);
                    host = (String) participant.get("host_url");
                    uri = (String) service.get("adapter_uri");
                    data = (JSONObject) new JSONParser().parse((String)request.get("request_data"));
                    //response =  sendPost(host+"/"+uri,data);
                    System.out.println(requestId+" "+host+"/"+uri+" "+data);
                    updateQueue(requestId, response);
                }

                try{
                    Thread.sleep(1000L);
                }catch(Exception e){}
            }while(true);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private void updateQueue(long requestId, JSONObject response) throws Exception{
        String sql = null;
        DBQuery query = null;

        sql = "update request_queue set response_data=?::json,status=? where request_id=?";
        query = new DBQuery(sql);
        query.setValue(Types.VARCHAR, response.toJSONString());
        query.setValue(Types.VARCHAR,"COMPLETED");
        query.setValue(Types.INTEGER,new Long(requestId).intValue()+"");
        DB.update(query);
    }

    public JSONObject sendPost(String url,JSONObject data) throws Exception {
        JSONObject res = null;
        String resstring = null;
        JSONParser parser = new JSONParser();
        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(data.toString()))
                .uri(URI.create(url))
                .setHeader("Content-Type", "application/json")
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        resstring = response.body();
        //System.out.println(resstring);
        res = (JSONObject) parser.parse(resstring);
        return res;
    }

    private JSONObject getParticipant(JSONArray participants, String participantId){
        JSONObject participant = null;
        Iterator<JSONObject> it = null;
        it = participants.iterator();
        while(it.hasNext()){
            participant = (JSONObject) it.next();
            if(participantId.equalsIgnoreCase((String)participant.get("participant_id"))){
                break;
            }
        }
        return participant;
    }

    private JSONObject getService(JSONArray services, String participantId, String serviceId, String versionNo){
        JSONObject service = null;
        Iterator<JSONObject> it = null;
        it = services.iterator();
        while(it.hasNext()){
            service = (JSONObject) it.next();
            if(participantId.equalsIgnoreCase((String)service.get("participant_id")) &&
                serviceId.equalsIgnoreCase((String)service.get("service_id")) &&
                    versionNo.equalsIgnoreCase((String)service.get("version_no"))){
                break;
            }
        }
        return service;
    }

    private JSONArray getQueue() throws Exception{
        String sql = null;
        DBQuery query = null;
        DBResult rs = null;
        JSONObject record = null;
        JSONArray result = new JSONArray();

        sql = "select * from request_queue where status = 'NEW' or status = 'PENDING'";
        query = new DBQuery( sql);
        rs = DB.fetch(query);
        while(rs.hasNext()){
            record = (JSONObject) rs.next();
            result.add(record);
        }
        return result;
    }

    public void contextDestroyed(ServletContextEvent servletContextEvent) {

    }
}


