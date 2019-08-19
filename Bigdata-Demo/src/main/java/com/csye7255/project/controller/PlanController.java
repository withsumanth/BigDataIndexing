package com.csye7255.project.controller;

import com.csye7255.project.Etag.EtagMap;
import com.csye7255.project.Exception.*;
import com.csye7255.project.service.PlanService;
import com.csye7255.project.service.TokenService;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.rest.webmvc.json.JsonSchema;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping
public class PlanController {

	@Autowired 
	private PlanService planService;
	
	@Autowired
    private TokenService tokenService;

	private HashMap<String, String> map = new HashMap<>();

	@PostMapping(path = "/plan", consumes = "application/json", produces = "application/json")
	@ResponseBody
	public ResponseEntity<String> addInsurancePlan(@RequestHeader HttpHeaders headers, @RequestBody String plan) {
		String token = headers.getFirst("Authorization");
		Map<String, String> validEtag = planService.createPlan(plan,token);
		String etag = validEtag.get("etag");
		System.out.println(etag);

		if (validEtag.size() > 1) {
			map.clear();
			map.put("message: ", "Data Saved. PlanId: " + validEtag.get("planid"));
			HttpHeaders headers1 = new HttpHeaders();
			headers1.add("ETag", etag);
			return new ResponseEntity(map, headers1, HttpStatus.CREATED);
		} else {
			map.clear();
			map.put("message: ", "Data already present");
			HttpHeaders headers1 = new HttpHeaders();
			headers1.add("ETag", etag);
			return new ResponseEntity(map, headers1, HttpStatus.ALREADY_REPORTED);
		}
	}

	@GetMapping(path = "/plan/{id}", produces = "application/json")
	public ResponseEntity<String> getInsurancePlan(@PathVariable(value = "id") String planId,
			@RequestHeader HttpHeaders header) {
		try {
			String token = header.getFirst("Authorization");
			if (planId != null) {
				String etag = header.getETag();

				Map<String, String> validEtag = planService.getPlan(planId, etag,token);

				return ResponseEntity.status(HttpStatus.OK).eTag(validEtag.get("etag")).body(validEtag.get("plan"));
			} else {
				map.clear();
				map.put("message: ", "PlanId not present");
				HttpHeaders headers = new HttpHeaders();
				headers.add("ETag", planService.getEtags(planId));
				return new ResponseEntity(map, headers, HttpStatus.BAD_REQUEST);
			}
		} catch (Exception ex) {
			map.clear();
			map.put("message: ", ex.getMessage());
			HttpHeaders headers = new HttpHeaders();
			headers.add("ETag", "\"" + planService.getEtags(planId) + "\"");
			HttpStatus status = planService.getStatus(ex);
			if (ex != null)
				return new ResponseEntity(map, headers, status);
			else
				throw ex;
		}
	}

	@DeleteMapping(path = "/plan/{id}")
	public ResponseEntity<String> deletePlan(@PathVariable(value = "id") String planId,
			@RequestHeader HttpHeaders headers) {
		map.clear();
		String token = headers.getFirst("Authorization");
		if(!tokenService.validateToken(token)) throw new BadRequest("Token is expired/Invalid");
		if (planId != null) {
			if ((new PlanService().deleteData(planId)) > 0) {
				planService.removeEtags(planId);
				map.put("message: ", "Delete Successful!");
				return new ResponseEntity(map, HttpStatus.OK);
			} else {
				map.put("message: ", "Plan Id Not Found");
				return new ResponseEntity(map, HttpStatus.BAD_REQUEST);
			}
		} else {
			map.put("message: ", "Enter the plan ID!!");
			return new ResponseEntity(map, HttpStatus.BAD_REQUEST);
		}
	}

	@PutMapping(path = "/plan/{id}")
	public ResponseEntity<String> updatePlan(@PathVariable(value = "id") String planId, @RequestBody String plan,
			@RequestHeader HttpHeaders header) {
		try {
			String token = header.getFirst("Authorization");
			String etag = null;
			 if (header.getIfMatch().size() > 0) {
				 if (header.getIfMatch().get(0) != null) {
						etag = header.getIfMatch().get(0);
						etag = etag.replace("\"", "");
					}
			 } else {
	                throw new Notmodified("ETag is not present");
	            }
			System.out.println(etag);
			Map<String, String> validEtag = planService.updatePlan(planId, etag, plan,token);
			etag = validEtag.get("etag");
			if (validEtag.size() > 1) {
				map.clear();
				map.put("message: ", "Data Saved Successfully. Plan id:" + validEtag.get("planid"));
				HttpHeaders headers = new HttpHeaders();
				headers.add("ETag", "\"" + etag + "\"");
				return new ResponseEntity(map, headers, HttpStatus.OK);
			} else {
				return ResponseEntity.status(HttpStatus.NOT_MODIFIED).eTag(etag).body("Data not updated");
			}
		} catch (Exception ex) {
			map.clear();
			map.put("message: ", ex.getMessage());
			HttpHeaders headers = new HttpHeaders();
			headers.add("ETag", "\"" + planService.getEtags(planId) + "\"");
			HttpStatus status = planService.getStatus(ex);
			if (ex != null)
				return new ResponseEntity(map, headers, status);
			else
				throw ex;
		}
	}

	@PatchMapping(path = "/plan/{id}", produces = "application/json")
	public ResponseEntity<String> patchPlan1(@PathVariable(value = "id") String planId, @RequestBody String plan,
			@RequestHeader HttpHeaders headers) throws Exception {
		try {
			String etag = null;
			String token = headers.getFirst("Authorization");
			if(!tokenService.validateToken(token)) throw new BadRequest("Token is expired/Invalid");
			if (headers.getIfMatch().size() > 0) {
				if (headers.getIfMatch().get(0) != null) {
					etag = headers.getIfMatch().get(0);
					etag = etag.replace("\"", "");
				}
			} else {
				throw new Notmodified("ETag is not present");
			}

			boolean success = planService.updatePlanPatch(planId, etag, plan);
			if (success) {
				map.clear();
				map.put("message: ", "Data Saved Successfully. Plan id: " + planId);
				HttpHeaders headers1 = new HttpHeaders();
				headers1.add("ETag", "\"" + planService.getEtags(planId) + "\"");
				return new ResponseEntity(map, headers1, HttpStatus.OK);
			} else {
				map.clear();
				map.put("message: ", "Update UnSuccessful ");
				HttpHeaders headers1 = new HttpHeaders();
				headers1.add("ETag", "\"" + planService.getEtags(planId) + "\"");
				return new ResponseEntity(map, headers1, HttpStatus.INTERNAL_SERVER_ERROR);
			}

		} catch (Exception ex) {
			map.clear();
			map.put("message: ", ex.getMessage());
			HttpHeaders headers1 = new HttpHeaders();
			headers1.add("ETag", "\"" + planService.getEtags(planId) + "\"");
			HttpStatus status = planService.getStatus(ex);
			if (ex != null)
				return new ResponseEntity(map, headers1, status);
			else
				throw ex;
		}
	}
}
