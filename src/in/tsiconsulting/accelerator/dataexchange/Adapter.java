package in.tsiconsulting.accelerator.dataexchange;

import in.tsiconsulting.accelerator.framework.InputProcessor;
import in.tsiconsulting.accelerator.framework.OutputProcessor;
import in.tsiconsulting.accelerator.framework.REST;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Iterator;

public class Adapter implements REST {

    @Override
    public void get(HttpServletRequest req, HttpServletResponse res) {
        System.out.println("Im here-get");
    }

    @Override
    public void post(HttpServletRequest req, HttpServletResponse res) {
        JSONObject input = null;
        JSONObject output = null;
        String requestId = null;
        String participantId = null;
        String serviceId = null;
        String versionNo = null;
        JSONArray services = null;
        JSONObject service = null;

        try {
            input = InputProcessor.getInput(req);
            requestId = req.getHeader("request-id");
            participantId = req.getHeader("participant-id");
            serviceId = req.getHeader("service-id");
            versionNo = req.getHeader("version-no");
            System.out.println("RequestId: "+requestId+" Receiving request:"+input);

            services = RegistrySync.getRegistry();

            service = getService(services, participantId, serviceId, versionNo);
            output = (JSONObject) new JSONParser().parse((String) service.get("success_sample_response"));
            System.out.println("RequestId: "+requestId+" Sending response:"+output);
        }catch(Exception e){
            OutputProcessor.sendError(res,HttpServletResponse.SC_INTERNAL_SERVER_ERROR,"Unknown server error");
            e.printStackTrace();
        }
        // send output
        OutputProcessor.send(res,HttpServletResponse.SC_OK,output);
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

    @Override
    public void delete(HttpServletRequest req, HttpServletResponse res) {

    }

    @Override
    public void put(HttpServletRequest req, HttpServletResponse res) {

    }

    @Override
    public boolean validate(String method, HttpServletRequest req, HttpServletResponse res) {
        // Add additional validation if required
        return true;
    }
}
