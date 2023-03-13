package in.tsiconsulting.accelerator.dataexchange;

import in.tsiconsulting.accelerator.framework.SystemConfig;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Properties;

public class RegistrySync implements ServletContextListener {

    private final java.net.http.HttpClient httpClient = java.net.http.HttpClient.newBuilder()
            .version(java.net.http.HttpClient.Version.HTTP_2)
            .build();

    private static JSONArray registry = null;

    public static JSONArray getRegistry(){
        return registry;
    }

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        SystemConfig.load(servletContextEvent.getServletContext());

        Processor obj = new Processor();
        Thread thread = new Thread(obj);
        thread.start();

    }

    public class Processor implements Runnable{

        @Override
        public void run() {
            do{
                try {
                    Properties config = SystemConfig.getAppConfig();
                    String serviceurl = config.getProperty("registry.host") + "/" + config.getProperty("registry.uri");
                    String authorization = "Basic "+config.getProperty("registry.authorization");
                    JSONObject data = new JSONObject();
                    data.put("_func", config.getProperty("registry.list.services.func"));
                    registry = sendPost(serviceurl, authorization, data);

                }catch(Exception e){
                    //e.printStackTrace();
                }
                if(registry == null)
                    System.out.println("Registry - Unavailable");
                try{
                    Thread.sleep(2000L);
                }catch(Exception e){}
            }while(registry == null);
            }
    }

    public JSONArray sendPost(String url, String authorization, JSONObject data) throws Exception {
        JSONArray res = null;
        String resstring = null;
        JSONParser parser = new JSONParser();
        HttpRequest request = HttpRequest.newBuilder()
                .POST(HttpRequest.BodyPublishers.ofString(data.toString()))
                .uri(URI.create(url))
                .setHeader("authorization", authorization)
                .setHeader("Content-Type", "application/json")
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        resstring = response.body();
        //System.out.println(resstring);
        res = (JSONArray) parser.parse(resstring);
        return res;
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {

    }
}
