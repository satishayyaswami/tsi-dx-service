package in.tsiconsulting.accelerator.dataexchange;

import in.tsiconsulting.accelerator.framework.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.sql.Types;

public class Service implements REST {

    private static final String FUNCTION = "_func";
    private static final String DATA = "_data";


    @Override
    public void get(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) {

    }

    @Override
    public void post(HttpServletRequest req, HttpServletResponse res) {
        JSONObject input = null;
        JSONObject output = null;

        try {
            input = InputProcessor.getInput(req);
            output = serve(input);
            OutputProcessor.send(res, HttpServletResponse.SC_OK, output);
        }catch(Exception e){
            OutputProcessor.sendError(res,HttpServletResponse.SC_INTERNAL_SERVER_ERROR,"Unknown server error");
            e.printStackTrace();
        }
    }

    private JSONObject serve(JSONObject input) throws Exception{
        JSONObject requestOb = new JSONObject();
        String sql = null;
        DBQuery query = null;
        String participantId = (String) input.get("participant_id");
        String serviceId = (String) input.get("service_id");
        String versionNo = (String) input.get("version_no");
        JSONObject data = (JSONObject) input.get("data");
        String status = "NEW";

        sql = "insert into request_queue (participant_id,service_id,version_no,request_data,status) values (?,?,?,?::json,?)";
        query = new DBQuery( sql);
        query.setValue(Types.VARCHAR,participantId);
        query.setValue(Types.VARCHAR,serviceId);
        query.setValue(Types.VARCHAR,versionNo);
        query.setValue(Types.VARCHAR,data.toJSONString());
        query.setValue(Types.VARCHAR,status);
        int id = DB.insert(query);
        requestOb.put("request-id",id);
        return requestOb;
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
