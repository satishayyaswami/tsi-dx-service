package in.tsiconsulting.accelerator.dataexchange;

import in.tsiconsulting.accelerator.framework.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.sql.Types;

public class ServiceRegistry implements REST {

    private static final String FUNCTION = "_func";
    private static final String DATA = "_data";

    private static final String REGISTER_SERVICE = "register_service";
    private static final String LIST_SERVICES = "list_services";

    @Override
    public void get(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) {

    }

    @Override
    public void post(HttpServletRequest req, HttpServletResponse res) {
        JSONObject input = null;
        JSONObject output = null;
        JSONArray outputArr = null;
        JSONObject scoredef = null;
        String func = null;
        String participantId = null;
        String serviceId = null;

        try {
            input = InputProcessor.getInput(req);
            func = (String) input.get(FUNCTION);
            //System.out.println("func:"+func);
            participantId = (String) input.get("participant_id");

            if(func != null){
                if(func.equalsIgnoreCase(REGISTER_SERVICE)){
                    output = registerService(input);
                }else if(func.equalsIgnoreCase(LIST_SERVICES)){
                    outputArr = listServices();
                }
            }
            if(outputArr != null){
                OutputProcessor.send(res, HttpServletResponse.SC_OK, outputArr);
            }else {
                OutputProcessor.send(res, HttpServletResponse.SC_OK, output);
            }
        }catch(Exception e){
            OutputProcessor.sendError(res,HttpServletResponse.SC_INTERNAL_SERVER_ERROR,"Unknown server error");
            e.printStackTrace();
        }
    }

    private JSONObject registerService(JSONObject input) throws Exception{
        JSONObject out = new JSONObject();
        String participantId = (String) input.get("participant_id");
        String serviceId = (String) input.get("service_id");

        if(serviceexists(participantId, serviceId)){
            // update
            updateService(input);
            out.put("updated",true);
        }else{
            // insert
            insertService(input);
            out.put("created",true);
        }
        return out;
    }

    private boolean serviceexists(String participantId, String serviceId) throws Exception{
        boolean exists = false;
        String sql = null;
        DBQuery query = null;
        int count = 0;

        sql = "select count(*) from service_registry where participant_id=? and service_id=?";
        query = new DBQuery( sql);
        query.setValue(Types.VARCHAR,participantId);
        query.setValue(Types.VARCHAR,serviceId);
        count = DB.fetchCount(query);
        if(count > 0) exists = true;

        return exists;
    }

    private void insertService(JSONObject input) throws Exception{
        String sql = null;
        DBQuery query = null;
        String participantId = (String) input.get("participant_id");
        String serviceId = (String) input.get("service_id");
        String versionNo = (String) input.get("version_no");
        String serviceNetwork = (String) input.get("service_network");
        String serviceName = (String) input.get("service_name");
        String serviceUri = (String) input.get("service_uri");
        String adapterUri = (String) input.get("adapter_uri");
        String callbackUrl = (String) input.get("callback_url");
        String serviceDesc = (String) input.get("service_desc");
        String dataTransferType = (String) input.get("data_transfer_type");
        JSONObject requestSchema = (JSONObject) input.get("request_schema");
        JSONObject successResponseSchema = (JSONObject) input.get("success_response_schema");
        JSONObject errorResponseSchema = (JSONObject) input.get("error_response_schema");
        JSONObject sampleRequest = (JSONObject) input.get("sample_request");
        JSONObject successSampleResponse = (JSONObject) input.get("success_sample_response");
        JSONObject errorSampleResponse = (JSONObject) input.get("error_sample_response");

        sql = "insert into service_registry (participant_id,service_id,version_no,service_network,service_name,service_uri,adapter_uri,callback_url,service_desc,data_transfer_type,request_schema,success_response_schema,error_response_schema,sample_request,success_sample_response,error_sample_response) values (?,?,?,?,?,?,?,?,?,?,?::json,?::json,?::json,?::json,?::json,?::json)";
        query = new DBQuery( sql);
        query.setValue(Types.VARCHAR,participantId);
        query.setValue(Types.VARCHAR,serviceId);
        query.setValue(Types.VARCHAR,versionNo);
        query.setValue(Types.VARCHAR,serviceNetwork);
        query.setValue(Types.VARCHAR,serviceName);
        query.setValue(Types.VARCHAR,serviceUri);
        query.setValue(Types.VARCHAR,adapterUri);
        query.setValue(Types.VARCHAR,callbackUrl);
        query.setValue(Types.VARCHAR,serviceDesc);
        query.setValue(Types.VARCHAR,dataTransferType);
        query.setValue(Types.VARCHAR,requestSchema.toJSONString());
        query.setValue(Types.VARCHAR,successResponseSchema.toJSONString());
        query.setValue(Types.VARCHAR,errorResponseSchema.toJSONString());
        query.setValue(Types.VARCHAR,sampleRequest.toJSONString());
        query.setValue(Types.VARCHAR,successSampleResponse.toJSONString());
        query.setValue(Types.VARCHAR,errorSampleResponse.toJSONString());
        DB.update(query);
    }

    private void updateService(JSONObject input) throws Exception{
        String sql = null;
        DBQuery query = null;
        String participantId = (String) input.get("participant_id");
        String serviceId = (String) input.get("service_id");
        String versionNo = (String) input.get("version_no");
        String serviceNetwork = (String) input.get("service_network");
        String serviceName = (String) input.get("service_name");
        String serviceUri = (String) input.get("service_uri");
        String adapterUri = (String) input.get("adapter_uri");
        String callbackUrl = (String) input.get("callback_url");
        String serviceDesc = (String) input.get("service_desc");
        String dataTransferType = (String) input.get("data_transfer_type");
        JSONObject requestSchema = (JSONObject) input.get("request_schema");
        JSONObject successResponseSchema = (JSONObject) input.get("success_response_schema");
        JSONObject errorResponseSchema = (JSONObject) input.get("error_response_schema");
        JSONObject sampleRequest = (JSONObject) input.get("sample_request");
        JSONObject successSampleResponse = (JSONObject) input.get("success_sample_response");
        JSONObject errorSampleResponse = (JSONObject) input.get("error_sample_response");

        sql = "update service_registry set participant_id=?,service_id=?,version_no=?,service_network=?,service_name=?,service_uri=?,adapter_uri=?,callback_url=?,service_desc=?,data_transfer_type=?,request_schema=?::json,success_response_schema=?::json,error_response_schema=?::json,sample_request=?::json,success_sample_response=?::json,error_sample_response=?::json where participant_id=? and service_id=?";
        query = new DBQuery(sql);
        query.setValue(Types.VARCHAR,participantId);
        query.setValue(Types.VARCHAR,serviceId);
        query.setValue(Types.VARCHAR,versionNo);
        query.setValue(Types.VARCHAR,serviceNetwork);
        query.setValue(Types.VARCHAR,serviceName);
        query.setValue(Types.VARCHAR,serviceUri);
        query.setValue(Types.VARCHAR,adapterUri);
        query.setValue(Types.VARCHAR,callbackUrl);
        query.setValue(Types.VARCHAR,serviceDesc);
        query.setValue(Types.VARCHAR,dataTransferType);
        query.setValue(Types.VARCHAR,requestSchema.toJSONString());
        query.setValue(Types.VARCHAR,successResponseSchema.toJSONString());
        query.setValue(Types.VARCHAR,errorResponseSchema.toJSONString());
        query.setValue(Types.VARCHAR,sampleRequest.toJSONString());
        query.setValue(Types.VARCHAR,successSampleResponse.toJSONString());
        query.setValue(Types.VARCHAR,errorSampleResponse.toJSONString());
        DB.update(query);
    }

    private JSONArray listServices() throws Exception{
        String sql = null;
        DBQuery query = null;
        JSONArray variables,grades = null;
        DBResult rs = null;
        JSONObject record = null;
        JSONArray result = new JSONArray();

        sql = "select * from service_registry";
        query = new DBQuery( sql);
        rs = DB.fetch(query);
        while(rs.hasNext()){
            record = (JSONObject) rs.next();
            result.add(record);
        }
        return result;
    }

    @Override
    public void delete(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) {

    }

    @Override
    public void put(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) {

    }

    @Override
    public boolean validate(String s, HttpServletRequest req, HttpServletResponse res) {
        return InputProcessor.validate( req, res);
    }
}
