package in.tsiconsulting.accelerator.dataexchange;

import in.tsiconsulting.accelerator.framework.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.sql.Types;
import java.util.Iterator;

public class NetworkParticipants implements REST {

    private static final String FUNCTION = "_func";
    private static final String DATA = "_data";

    private static final String REGISTER_PARTICIPANT = "register_participant";
    private static final String LIST_PARTICIPANTS = "list_participants";


    @Override
    public void get(HttpServletRequest req, HttpServletResponse res) {

    }

    @Override
    public void post(HttpServletRequest req, HttpServletResponse res) {
        JSONObject input = null;
        JSONObject output = null;
        JSONArray outputArr = null;
        JSONObject scoredef = null;
        String func = null;
        String participantId = null;

        try {
            input = InputProcessor.getInput(req);
            func = (String) input.get(FUNCTION);
            //System.out.println("func:"+func);
            participantId = (String) input.get("participant_id");

            if(func != null){
                if(func.equalsIgnoreCase(REGISTER_PARTICIPANT)){
                    output = registerParticipant(input);
                }else if(func.equalsIgnoreCase(LIST_PARTICIPANTS)){
                    outputArr = getParticipants();
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

    private JSONObject registerParticipant(JSONObject input) throws Exception{
        JSONObject out = new JSONObject();
        String participantId = (String) input.get("participant_id");

        if(participantexists(participantId)){
            // update
            updateParticipant(input);
            out.put("updated",true);
        }else{
            // insert
            insertParticipant(input);
            out.put("created",true);
        }
        return out;
    }

    private boolean participantexists(String participantId) throws Exception{
        boolean exists = false;
        String sql = null;
        DBQuery query = null;
        int count = 0;

        sql = "select count(*) from network_participants_registry where participant_id=?";
        query = new DBQuery( sql);
        query.setValue(Types.VARCHAR,participantId);
        count = DB.fetchCount(query);
        if(count > 0) exists = true;

        return exists;
    }

    private void insertParticipant(JSONObject input) throws Exception{
        String sql = null;
        DBQuery query = null;
        String participantId = (String) input.get("participant_id");
        String participantName = (String) input.get("participant_name");
        String participantBusinessDesc = (String) input.get("participant_business_desc");
        String participantType = (String) input.get("participant_type");
        String hostUrl = (String) input.get("host_url");
        String publicKey = (String) input.get("public_key");

        sql = "insert into network_participants_registry (participant_id,participant_name,participant_business_desc,participant_type,host_url,public_key) values (?,?,?,?,?,?)";
        query = new DBQuery( sql);
        query.setValue(Types.VARCHAR,participantId);
        query.setValue(Types.VARCHAR,participantName);
        query.setValue(Types.VARCHAR,participantBusinessDesc);
        query.setValue(Types.VARCHAR,participantType);
        query.setValue(Types.VARCHAR,hostUrl);
        query.setValue(Types.VARCHAR,publicKey);
        DB.update(query);
    }

    private void updateParticipant(JSONObject input) throws Exception{
        String sql = null;
        DBQuery query = null;
        String participantId = (String) input.get("participant_id");
        String participantName = (String) input.get("participant_name");
        String participantBusinessDesc = (String) input.get("participant_business_desc");
        String participantType = (String) input.get("participant_type");
        String hostUrl = (String) input.get("host_url");
        String publicKey = (String) input.get("public_key");

        sql = "update network_participants_registry set participant_name=?,participant_business_desc=?,participant_type=?,host_url=?,public_key=? where participant_id=?";
        query = new DBQuery(sql);
        query.setValue(Types.VARCHAR,participantId);
        query.setValue(Types.VARCHAR,participantName);
        query.setValue(Types.VARCHAR,participantBusinessDesc);
        query.setValue(Types.VARCHAR,participantType);
        query.setValue(Types.VARCHAR,hostUrl);
        query.setValue(Types.VARCHAR,publicKey);
        DB.update(query);
    }

    private JSONArray getParticipants() throws Exception{
        String sql = null;
        DBQuery query = null;
        JSONArray variables,grades = null;
        DBResult rs = null;
        JSONObject record = null;
        JSONArray result = new JSONArray();

        sql = "select * from network_participants_registry";
        query = new DBQuery( sql);
        rs = DB.fetch(query);
        while(rs.hasNext()){
            record = (JSONObject) rs.next();
            result.add(record);
        }
        return result;
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
        return InputProcessor.validate( req, res);
    }
}
