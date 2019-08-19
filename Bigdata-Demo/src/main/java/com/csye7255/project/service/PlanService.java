package com.csye7255.project.service;

import com.csye7255.project.Exception.*;
import com.csye7255.project.Etag.EtagMap;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class PlanService {

	Map<String, String> out = new HashMap<>();
	
	@Autowired
    private TokenService tokenService;

	private static JedisPool jedisPool = new JedisPool("localhost", 6379);
	private static Map<String, String> map = new HashMap<>();
	static String IndexQueue = "RedisIndexQueue";
	private RestHighLevelClient restHighLevelClient = new RestHighLevelClient(
			RestClient.builder(new HttpHost("localhost", 9200))
					.setRequestConfigCallback(
							requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(5000).setSocketTimeout(5000))
					.setMaxRetryTimeoutMillis(60000));

	public static Map<String, String> retrieveMap(JSONObject jsonData) {
		nestedObject(jsonData);
		return map;
	}

	public static Object nestedObject(JSONObject jsonNestedObject) {
		JSONObject object = new JSONObject();
		JSONArray array = new JSONArray();
		for (Object key : jsonNestedObject.keySet()) {
			if (jsonNestedObject.get((String) key) instanceof JSONObject) {
				object.put((String) key, nestedObject(jsonNestedObject.getJSONObject((String) key)));
			} else if (jsonNestedObject.get((String) key) instanceof JSONArray) {
				JSONArray arr = jsonNestedObject.getJSONArray((String) key);
				for (int i = 0; i < arr.length(); i++) {
					array.put(i, nestedObject(arr.getJSONObject(i)));
				}
				object.put((String) key, array.toString());
			} else {
				object.put((String) key, jsonNestedObject.get((String) key));
			}
		}
		if (!object.keySet().isEmpty())
			map.put(jsonNestedObject.get("objectType") + "_" + jsonNestedObject.get("objectId"), object.toString());
		EtagMap.getEtags().put(jsonNestedObject.get("objectType") + "_" + jsonNestedObject.get("objectId"),
				UUID.randomUUID().toString());
		return jsonNestedObject.get("objectType") + "_" + jsonNestedObject.get("objectId");
	}

	public JSONObject readData(String planData) {
		Jedis jedis = jedisPool.getResource();
		JSONObject object = new JSONObject(planData);
		for (Object key : object.keySet()) {
			try {
				if (((String) object.get((String) key)).contains("[")) {
					JSONArray array = new JSONArray((String) object.get((String) key));
					for (int i = 0; i < array.length(); i++) {
						array.put(i, readData(jedis.get((String) array.get(i))));
					}
					object.put((String) key, array);
				}
				if (jedis.get(object.getString((String) key)) != null) {
					if (jedis.get((String) object.get((String) key)) != null) {
						object.put((String) key, readData(jedis.get((String) object.get((String) key))));
					}
				}
			} catch (Throwable ex) {
				System.out.println(ex.getMessage());
			}
		}
		jedis.close();
		return object;
	}

	public long deleteData(String planId) {
		Jedis jedis = jedisPool.getResource();
		System.out.println(jedis.get(planId));
		String id = "";
		String type = "";
		try {
			if (planId != null) {
				id = planId.split("_")[1];
				type = planId.split("_")[0];
			}
			restHighLevelClient.delete(new DeleteRequest("plan_index", "plan", id));
		} catch (IOException e) {
			System.out.println("Exception during elastic search delete" + e.getMessage());
		}

		if (jedis.get(planId) != null) {
			JSONObject object = new JSONObject(jedis.get(planId));
			for (Object key : object.keySet()) {
				if (String.valueOf(object.get((String) key)).contains("[")) {
					JSONArray array = new JSONArray(String.valueOf(object.get((String) key)));
					for (int i = 0; i < array.length(); i++) {
						deleteData(array.getString(i));
					}
				}
				if (jedis.get(String.valueOf(object.get((String) key))) != null) {
					System.out.println(jedis.get(object.getString((String) key)));
					deleteData(object.getString((String) key));
				}
			}
			long status = jedis.del(planId);
			jedis.close();
			return status;
		} else {
			return -2;
		}
	}

	public boolean validateJson(JSONObject jsonData) throws FileNotFoundException {
		System.out.println("Inside validateschema");
		BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/resources/static/jsonschema.json"));
		JSONObject jsonSchema = new JSONObject(new JSONTokener(bufferedReader));
		Schema schema = SchemaLoader.load(jsonSchema);
		try {
			schema.validate(jsonData);
			return true;
		} catch (ValidationException e) {
			throw new ExpectationFailed("Enter correct input! The issue is present in " + e.getMessage());
		}
	}

	public String validateJson1(JSONObject jsonData) throws Exception {
		BufferedReader bufferedReader = new BufferedReader(new FileReader("src/main/resources/static/Service.json"));
		JSONObject jsonSchema = new JSONObject(new JSONTokener(bufferedReader));
		System.out.println(jsonSchema.toString());
		Schema schema = SchemaLoader.load(jsonSchema);
		try {
			schema.validate(jsonData);
			return null;
		} catch (ValidationException e) {
			throw new ExpectationFailed("Enter correct input! The issue is present in " + e.getMessage());
		}
	}

	public void removeEtags(String planId) {
		Set<String> set = EtagMap.getEtags().keySet().stream().filter(s -> s.startsWith(planId))
				.collect(Collectors.toSet());
		if (!set.isEmpty()) {
			EtagMap.getEtags().keySet().removeAll(set);
		}
	}

	public String getEtags(String planId) {
		String etag = "";
		if (!planId.contains("plan_")) {
			if(EtagMap.getEtags().size()!=0) {
				String mplanId = EtagMap.getEtags().keySet().stream().filter(s -> s.endsWith("p"))
						.collect(Collectors.toList()).get(0);
				etag = EtagMap.getEtags().get(mplanId);
			}
		} else {
			etag = EtagMap.getEtags().get(planId + "p");
			if (etag == null) {
				removeEtags(planId);
				etag = UUID.randomUUID().toString();
				EtagMap.getEtags().put(planId + "p", etag);
				return etag;
			}
		}
		return etag;
	}

	public Map<String, String> createPlan(String plan,String token) {
		if(!tokenService.validateToken(token)) throw new BadRequest("Token is expired/Invalid");
		out.clear();
		Jedis jedis = null;
		JSONObject jsonData = new JSONObject(new JSONTokener((new JSONObject(plan)).toString()));
		System.out.println(jsonData);
		try {
			if (validateJson(jsonData)) {
				jedis = jedisPool.getResource();
				String key = jsonData.get("objectType") + "_" + jsonData.get("objectId");
				if (jedis.get(key) == null) {
					Map<String, String> data = PlanService.retrieveMap(jsonData);
					for (Map.Entry entry : data.entrySet()) {
						jedis.set((String) entry.getKey(), (String) entry.getValue());
					}
					jedis.rpush(IndexQueue.getBytes(), jsonData.toString().getBytes(StandardCharsets.UTF_8));
					String etag = UUID.randomUUID().toString();
					EtagMap.getEtags().put(key + "p", etag);
					out.put("planid", key);
					out.put("etag", etag);
					return out;
				} else {
					out.put("etag", EtagMap.getEtags().get(key + "p"));
					return out;
				}
			} else
				throw new ExpectationFailed("Enter correct input!");
		} catch (Exception e) {
			throw new BadRequest(e.getMessage());
		} finally {
			if (jedis != null)
				jedis.close();
		}
	}

	public Map<String, String> getPlan(String id, String etag, String token) {
		out.clear();
		if(!tokenService.validateToken(token)) throw new BadRequest("Token is expired/Invalid");
		//if (etag != null) {
			/*if (EtagMap.getEtags().containsKey(id + "g")) {
				if (EtagMap.getEtags().get(id + "g").equals(etag)) {
					throw new Notmodified("Data has not been updated since last time!");
				} else if (!EtagMap.getEtags().get(id + "g").equals(etag)) {
					EtagMap.getEtags().remove(EtagMap.getEtags().get(id + "g"));
					EtagMap.getEtags().put(id + "g", etag);
				}

			}*/
			Jedis jedis = jedisPool.getResource();
			String plan = jedis.get(id);
			if (plan == null)
				throw new ResourceNotFound("plan", "id", id);
			/*if (EtagMap.getEtags().get(id + "p") == null) {
				etag = getEtags(id);
			} else {
				etag = etag != null ? etag : EtagMap.getEtags().get(id + "p");
			}*/
			//EtagMap.getEtags().put(id + "g", etag);
			out.put("plan", readData(plan).toString());
			//out.put("etag", EtagMap.getEtags().get(id + "p"));
			jedis.close();
			return out;
		//}// else
			//throw new PreconditionFailed("Etag is not present");
	}

	public Map<String, String> updatePlan(String planId, String etag, String plan,String token) {
		out.clear();
		if(!tokenService.validateToken(token)) throw new BadRequest("Token is expired/Invalid");
		if (planId == null)
			throw new BadRequest("PlanId cannot be Empty");
		if (etag == null || etag.equals(""))
			throw new PreconditionFailed(
					"Data has been updated by other User. Please GET the updated data and then update it!");
		if (EtagMap.getEtags().containsKey(planId + "pu")) {
			if (etag.equals(EtagMap.getEtags().get(planId + "pu")))
				throw new Notmodified("Data has not been updated since last time!");
		}
		if (EtagMap.getEtags().containsKey(planId + "p")) {
			if (!etag.equals(EtagMap.getEtags().get(planId + "p")))
				throw new Forbidden(
						"Data has been updated by other User. Please GET the updated data and then update it!");
			Jedis jedis = jedisPool.getResource();
			JSONObject jsonData = new JSONObject(new JSONTokener((new JSONObject(plan)).toString()));
			if (jedis.get(planId) != null) {
				try {
					if (validateJson(jsonData)) {
						if ((deleteData(planId)) > 0) {
							removeEtags(planId);
							jedis.close();
							out = createPlan(plan,token);
							etag = UUID.randomUUID().toString();
							EtagMap.getEtags().put(planId + "p", etag);
							out.replace("etag", etag);
							return out;
						} else
							throw new BadRequest("Update Failed");
					}else throw new ExpectationFailed("Enter correct input!");
				} catch (Exception e) {
	                throw new BadRequest(e.getMessage());
	            } finally {
	                if (jedis != null)
	                    jedis.close();
	            }
			} else {
				jedis.close();
				throw new BadRequest("No such content found!!");
			}
		} else
			throw new PreconditionFailed(
					"Data has been updated by other User. Please GET the updated data and then update it!");
	}

	public boolean updatePlanPatch(String planId, String etag, String plan) throws Exception {
		Jedis jedis = jedisPool.getResource();
		JSONObject input = new JSONObject(plan);
		if (!planId.contains("plan_") || !input.keySet().contains("objectId")) {
			String mplanId = EtagMap.getEtags().keySet().stream().filter(s -> s.endsWith("p"))
					.collect(Collectors.toList()).get(0);
			if (mplanId != null && EtagMap.getEtags().containsKey(mplanId)) {
				if (!etag.equals(EtagMap.getEtags().get(mplanId))) {
					jedis.close();
					throw new Forbidden(
							"Data has been updated by other User. Please GET the updated data and then update it!");
				}
			}
		} else {
			if (EtagMap.getEtags().containsKey(planId + "p")) {
				if (!(etag.equals(EtagMap.getEtags().get(planId + "p")))) {
					jedis.close();
					throw new Forbidden(
							"Data has been updated by other User. Please GET the updated data and then update it!");
				}
			}
		}

		if (!planId.contains("plan_") || !input.keySet().contains("objectId")) {
			JSONObject data = new JSONObject(jedis.get(planId));
			for (Object key : input.keySet()) {
				if (!data.get((String) key).equals(input.get((String) key))) {
					data.put(key.toString(), input.get((String) key));
				}
			}
			jedis.set(planId, data.toString());

			String skey = null;
			Set<String> set = EtagMap.getEtags().keySet().stream().filter(s -> s.endsWith("p"))
					.collect(Collectors.toSet());
			if (!set.isEmpty()) {
				EtagMap.getEtags().keySet().removeAll(set);
				for (String s : set) {
					skey = s;
				}
			}

			etag = UUID.randomUUID().toString();
			EtagMap.getEtags().put(skey, etag);

		} else {

			if (input.getString("objectType").equalsIgnoreCase("planservice")) {
				String message = validateJson1(input);
				if (message != null) {
					jedis.close();
					throw new ExpectationFailed("Enter correct input!");
				}
				Map<String, String> data = PlanService.retrieveMap(input);
				for (Map.Entry entry : data.entrySet()) {
					jedis.set((String) entry.getKey(), (String) entry.getValue());
				}
				String fullPlan = jedis.get(planId);

				System.out.println(fullPlan + "\n\n\n");
				String[] split = fullPlan.split("\\[");
				String x = "[\\\"" + input.getString("objectType") + "_" + input.getString("objectId") + "\\\",";
				fullPlan = split[0] + x + split[1];
				jedis.set(planId, fullPlan);
				removeEtags(planId);
				etag = UUID.randomUUID().toString();
				EtagMap.getEtags().put(planId + "p", etag);
				System.out.println(new PlanService().readData(fullPlan).toString() + "\n xxx" + planId.split("_")[1]);
				// restHighLevelClient.delete(new DeleteRequest("plan_index", "plan",
				// planId.split("_")[1]));
				// restHighLevelClient.close();
				String IndexQueue = "RedisIndexQueue";
				jedis.rpush(IndexQueue.getBytes(),
						new PlanService().readData(fullPlan).toString().getBytes(StandardCharsets.UTF_8));
				jedis.close();
				return true;
			}

		}
		jedis.close();
		return true;
	}

	public HttpStatus getStatus(Exception ex) {
		if (ex instanceof BadRequest) {
			BadRequest runex = (BadRequest) ex;
			return runex.getStatus();
		} else if (ex instanceof Notmodified) {
			Notmodified runex = (Notmodified) ex;
			return runex.getStatus();
		} else if (ex instanceof PreconditionFailed) {
			PreconditionFailed runex = (PreconditionFailed) ex;
			return runex.getStatus();
		} else if (ex instanceof Forbidden) {
			Forbidden runex = (Forbidden) ex;
			return runex.getStatus();
		} else if (ex instanceof ExpectationFailed) {
			ExpectationFailed runex = (ExpectationFailed) ex;
			return runex.getStatus();
		} else if (ex instanceof ResourceNotFound) {
			ResourceNotFound runex = (ResourceNotFound) ex;
			return runex.getStatus();
		} else if (ex instanceof JSONException){
			return HttpStatus.BAD_REQUEST;
		}else
			return null;

	}
}
