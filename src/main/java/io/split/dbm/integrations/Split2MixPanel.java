package io.split.dbm.integrations;

import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.binary.Base64;
import org.json.JSONObject;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.gson.Gson;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;

public class Split2MixPanel implements HttpFunction {
	
	private static final OkHttpClient client = new OkHttpClient();

	@Override
	public void service(HttpRequest request, HttpResponse response) throws Exception {
		long start = System.currentTimeMillis();
		GZIPInputStream gzIs = new GZIPInputStream(request.getInputStream());
		Impression[] impressions = new Gson().fromJson(new InputStreamReader(gzIs), Impression[].class);
		System.out.println("successfully parsed " + impressions.length + " impressions in " + (System.currentTimeMillis() - start) + "ms");
				
        long startMixPanel = System.currentTimeMillis();
		List<MixPanelEvent> events = new LinkedList<MixPanelEvent>();
        for(Impression impression : impressions) {
        	MixPanelEvent event = new MixPanelEvent();
        	event.event = "$experiment_started";
        	event.properties = new HashMap<String, Object>();
        	event.properties.put("Experiment name", impression.split);
        	event.properties.put("Variant name", impression.treatment);
        	event.properties.put("$source", "Split");

        	event.properties.put("split", impression.split);
        	event.properties.put("distinct_id", impression.key);
        	//event.properties.put("token", YOUR_MIXPANEL_TOKEN_HERE);
        	event.properties.put("token", "224979a8fcc23c2624902681bf9c206e");
        	event.properties.put("time", impression.time / 1000);
        	event.properties.put("treatment", impression.treatment);
        	event.properties.put("label", impression.label);
        	event.properties.put("environmentId", impression.environmentId);
        	event.properties.put("environmentName", impression.environmentName);
        	event.properties.put("sdk", impression.sdk);
        	event.properties.put("sdkVersion", impression.sdkVersion);
        	event.properties.put("splitVersionNumber", impression.splitVersionNumber);
        	
        	events.add(event);
        }
        String rawJson = new Gson().toJson(events);
        Base64 base64 = new Base64();
        String encodedJson = new String(base64.encode(rawJson.getBytes()));
        String body = "data=" + encodedJson;
        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        RequestBody requestBody = RequestBody.create(JSON, body);
        System.out.println("encoded MixPanel events in " + (System.currentTimeMillis() - startMixPanel) + "ms");
        
        long startPost = System.currentTimeMillis();
        Request req = new Request.Builder()
        		.url("http://api.mixpanel.com/track/")
        		.post(requestBody)
        		.build();
        
        client.newCall(req).execute();
        System.out.println("POSTed MixPanel events in " + (System.currentTimeMillis() - startPost) + "ms");

    	PrintWriter writer = new PrintWriter(response.getWriter());
    	writer.println("" + impressions.length + " impressions accepted and posted to MixPanel");
    	writer.flush();
    	writer.close();
    	System.out.println("finished in " + (System.currentTimeMillis() - start) + "ms");
	}
    
}

class MixPanelEvent {
	String event;
	Map<String, Object> properties;
	
	public String getEvent() {
		return event;
	}
	public void setEvent(String event) {
		this.event = event;
	}
	public Map<String, Object> getProperties() {
		return properties;
	}
	public void setProperties(Map<String, Object> properties) {
		this.properties = properties;
	}
	
	
}

class Impression {

	String key;
	String split;
	String environmentId;
	String environmentName;
	String treatment;
	long time;
	String bucketingKey;
	String label;
	String machineName;
	String machineIp;
	long splitVersionNumber;
	String sdk;
	String sdkVersion;
	String properties;

	public Impression() {

	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getSplit() {
		return split;
	}

	public void setSplit(String split) {
		this.split = split;
	}

	public String getEnvironmentId() {
		return environmentId;
	}

	public void setEnvironmentId(String environmentId) {
		this.environmentId = environmentId;
	}

	public String getEnvironmentName() {
		return environmentName;
	}

	public void setEnvironmentName(String environmentName) {
		this.environmentName = environmentName;
	}

	public String getTreatment() {
		return treatment;
	}

	public void setTreatment(String treatment) {
		this.treatment = treatment;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public String getBucketingKey() {
		return bucketingKey;
	}

	public void setBucketingKey(String bucketingKey) {
		this.bucketingKey = bucketingKey;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getMachineName() {
		return machineName;
	}

	public void setMachineName(String machineName) {
		this.machineName = machineName;
	}

	public String getMachineIp() {
		return machineIp;
	}

	public void setMachineIp(String machineIp) {
		this.machineIp = machineIp;
	}

	public long getSplitVersionNumber() {
		return splitVersionNumber;
	}

	public void setSplitVersionNumber(long splitVersionNumber) {
		this.splitVersionNumber = splitVersionNumber;
	}

	public String getSdk() {
		return sdk;
	}

	public void setSdk(String sdk) {
		this.sdk = sdk;
	}

	public String getSdkVersion() {
		return sdkVersion;
	}

	public void setSdkVersion(String sdkVersion) {
		this.sdkVersion = sdkVersion;
	}

	public String getProperties() {
		return properties;
	}

	public void setProperties(String properties) {
		this.properties = properties;
	}

	public String
	toJson() {
		JSONObject result = new JSONObject();

		result.put("key", getKey());
		result.put("split",getSplit());
		result.put("environmentId", getEnvironmentId());
		result.put("environmentName", getEnvironmentName());
		result.put("treatment", getTreatment());
		result.put("time", getTime());
		result.put("bucketingKey", getBucketingKey());
		result.put("label", getLabel());
		result.put("machineName", getMachineName());
		result.put("machineIp", getMachineIp());
		result.put("splitVersionNumber", getSplitVersionNumber());
		result.put("sdk", getSdk());
		result.put("sdkVersion", getSdkVersion());
		result.put("properties", getProperties());

		return result.toString(2);
	}

	public Impression(String key, String split, String environmentId, String environmentName, String treatment,
			long time, String bucketingKey, String label, String machineName, String machineIp,
			long splitVersionNumber, String sdk, String sdkVersion, String properties) {
		super();
		this.key = key;
		this.split = split;
		this.environmentId = environmentId;
		this.environmentName = environmentName;
		this.treatment = treatment;
		this.time = time;
		this.bucketingKey = bucketingKey;
		this.label = label;
		this.machineName = machineName;
		this.machineIp = machineIp;
		this.splitVersionNumber = splitVersionNumber;
		this.sdk = sdk;
		this.sdkVersion = sdkVersion;
		this.properties = properties;
	}
    
}
