package cloud.localstack;

import cloud.localstack.lambda.DDBEventParser;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.Record;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.util.StringInputStream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Simple implementation of a Java Lambda function executor.
 *
 * @author Waldemar Hummer
 */
@SuppressWarnings("restriction")
public class LambdaExecutor {
	private static final Logger LOG = Logger.getLogger(LambdaExecutor.class.getName());

	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception {
		if(args.length < 3) {
			System.err.println("Usage: java " + LambdaExecutor.class.getSimpleName() +
					" <lambdaClass> <handlerMethodName> <lambdaContextJson> <recordsFilePath>");
			System.exit(1);
		}
		String lambdaClassName = args[0];
		String handlerMethodName = args[1];
		String lambdaContextJson = args[2];
		String recordsFilePath = args[3];

		ObjectMapper contextReader = new ObjectMapper();
		Map<String, String> contextMap = contextReader.readerFor(Map.class).readValue(lambdaContextJson);
		Context ctx = new LambdaContext(
				contextMap.get("functionName"),
				contextMap.get("functionVersion"),
				contextMap.get("invokedFunctionARN"));

		String fileContent = readFile(recordsFilePath);
		ObjectMapper reader = new ObjectMapper();
		Map<String,Object> map = reader.readerFor(Map.class).readValue(fileContent);

		List<Map<String,Object>> records = (List<Map<String, Object>>) get(map, "Records");
		Object inputObject = map;

		Object handler = getHandler(lambdaClassName);
		if (records == null) {
			Optional<Object> deserialisedInput = getInputObject(reader, fileContent, handler);
			if (deserialisedInput.isPresent()) {
				inputObject = deserialisedInput.get();
			}
		} else {
			if (records.stream().anyMatch(record -> record.containsKey("kinesis"))) {
				handleKinesis(ctx, handler, handlerMethodName, records, fileContent);
				return;
			} else if (records.stream().anyMatch(record -> record.containsKey("Sns"))) {
				SNSEvent snsEvent = new SNSEvent();
				inputObject = snsEvent;
				snsEvent.setRecords(new LinkedList<>());
				for (Map<String, Object> record : records) {
					SNSEvent.SNSRecord r = new SNSEvent.SNSRecord();
					snsEvent.getRecords().add(r);
					SNSEvent.SNS snsRecord = new SNSEvent.SNS();
					Map<String, Object> sns = (Map<String, Object>) get(record, "Sns");
					snsRecord.setMessage((String) get(sns, "Message"));
					snsRecord.setMessageAttributes((Map<String, SNSEvent.MessageAttribute>) get(sns, "MessageAttributes"));
					snsRecord.setType("Notification");
					snsRecord.setTimestamp(new DateTime());
					r.setSns(snsRecord);
				}
			} else if (records.stream().filter(record -> record.containsKey("dynamodb")).count() > 0) {

				inputObject = DDBEventParser.parse(records);

			}
			//TODO: Support other events (S3, SQS...)
		}
		if (handler instanceof RequestHandler) {
			Object result = ((RequestHandler<Object, ?>) handler).handleRequest(inputObject, ctx);
			// try turning the output into json
			try {
				result = new ObjectMapper().writeValueAsString(result);
			} catch (JsonProcessingException jsonException) {
				// continue with results as it is
			}
			// The contract with lambci is to print the result to stdout, whereas logs go to stderr
			System.out.println(result);
		} else if (handler instanceof RequestStreamHandler) {
			OutputStream os = new ByteArrayOutputStream();
			((RequestStreamHandler) handler).handleRequest(
					new StringInputStream(fileContent), os, ctx);
			System.out.println(os);
		}
	}

	private static void handleKinesis(Context ctx, Object handler, String handlerMethodName,
									 List<Map<String,Object>> records, String fileContent) {
		KinesisEvent kinesisEvent = new KinesisEvent();
		kinesisEvent.setRecords(new LinkedList<>());
		for (Map<String, Object> record : records) {
			KinesisEventRecord r = new KinesisEventRecord();
			// Add all of the metadata to the record.
			r.setAwsRegion((String) record.get("awsRegion"));
			r.setEventSource((String) record.get("eventSource"));
			r.setEventSourceARN((String) record.get("eventSourceARN"));
			r.setEventID((String) record.get("eventID"));
			r.setEventName((String) record.get("eventName"));

			// Add the Kinesis records themselves.
			kinesisEvent.getRecords().add(r);
			Record kinesisRecord = new Record();
			Map<String, Object> kinesis = (Map<String, Object>) get(record, "kinesis");
			String dataString = new String(get(kinesis, "data").toString().getBytes());
			byte[] decodedData = Base64.getDecoder().decode(dataString);
			kinesisRecord.setData(ByteBuffer.wrap(decodedData));
			kinesisRecord.setPartitionKey((String) get(kinesis, "partitionKey"));
			kinesisRecord.setApproximateArrivalTimestamp(new Date());
			r.setKinesis(kinesisRecord);
		}

		// Lambda supports two handler types for Java:
		// 1) Custom POJO Input/Output with a fixed handler name ('handleRequest').
		// 2) Custom handler function definitions that match specific signatures.

		// Check for the first 'POJO' type.
		Object result = null;
		if (handler instanceof RequestHandler) {
			result = ((RequestHandler<Object, ?>) handler).handleRequest(kinesisEvent, ctx);
			// try turning the output into json
			try {
				result = new ObjectMapper().writeValueAsString(result);
			} catch (JsonProcessingException jsonException) {
				// continue with results as it is
			}
			System.out.println(result);
			return;
		}

		// Try matching the handler against the known handler signatures:
		// https://docs.aws.amazon.com/lambda/latest/dg/java-programming-model-req-resp.html
		Method kinesisMethod = getKinesisMethod(handler, handlerMethodName);
		if (kinesisMethod != null) {
			try {
				result = kinesisMethod.invoke(handler, kinesisEvent, ctx);
				System.out.println(result);
			} catch (IllegalAccessException|InvocationTargetException e) {
				LOG.warning(e.toString());
			}
			return;
		}

		Method streamMethod = getStreamMethod(handler, handlerMethodName);
		if (streamMethod != null) {
			StringInputStream inputStream;
			try {
				inputStream = new StringInputStream(fileContent);
			} catch (UnsupportedEncodingException e) {
				LOG.warning(e.toString());
				return;
			}

			try {
				Method method = handler.getClass().getMethod(
						handlerMethodName, InputStream.class, OutputStream.class, Context.class);
				OutputStream outputStream = new ByteArrayOutputStream();
				result = method.invoke(handler, inputStream, outputStream, ctx);
			} catch (NoSuchMethodException|IllegalAccessException|InvocationTargetException e) {
				LOG.warning("unable to invoke handler method");
				LOG.warning(e.toString());
				return;
			}
		}
		// The contract with lambci is to print the result to stdout, whereas logs go to stderr
		if (result == null) {
			LOG.warning("lambda handler did not match any known method signature");
			return;
		}

		// The contract with lambci is to print the result to stdout, whereas logs go to stderr
		System.out.println(result);
	}

	private static Method getKinesisMethod(Object handler, String handlerMethodName) {
		try {
			return handler.getClass().getMethod(handlerMethodName, KinesisEvent.class, Context.class);
		} catch (NoSuchMethodException e) {
			return null;
		}
	}

	private static Method getStreamMethod(Object handler, String handlerMethodName) {
		try {
			return handler.getClass().getMethod(
					handlerMethodName, InputStream.class, OutputStream.class, Context.class);
		} catch (NoSuchMethodException e) {
			return null;
		}
	}

	private static Optional<Object> getInputObject(ObjectMapper mapper, String objectString, Object handler) {
		Optional<Object> inputObject = Optional.empty();
		try {
			Optional<Type> handlerInterface = Arrays.stream(handler.getClass().getGenericInterfaces())
					.filter(genericInterface ->
						((ParameterizedTypeImpl) genericInterface).getRawType().equals(RequestHandler.class))
					.findFirst();
			if (handlerInterface.isPresent()) {
				Class<?> handlerInputType = Class.forName(((ParameterizedTypeImpl) handlerInterface.get())
						.getActualTypeArguments()[0].getTypeName());
				inputObject = Optional.of(mapper.readerFor(handlerInputType).readValue(objectString));
			}
		} catch (Exception genericException) {
			// do nothing
		}
		return inputObject;
	}

	private static Object getHandler(String handlerName) throws NoSuchMethodException, IllegalAccessException,
		InvocationTargetException, InstantiationException, ClassNotFoundException {
		Class<?> clazz = Class.forName(handlerName);
		return clazz.getConstructor().newInstance();
	}

	public static <T> T get(Map<String,T> map, String key) {
		T result = map.get(key);
		if(result != null) {
			return result;
		}
		key = StringUtils.uncapitalize(key);
		result = map.get(key);
		if(result != null) {
			return result;
		}
		return map.get(key.toLowerCase());
	}

	public static String readFile(String file) throws Exception {
		if(!file.startsWith("/")) {
			file = System.getProperty("user.dir") + "/" + file;
		}
		return Files.lines(Paths.get(file), StandardCharsets.UTF_8).collect(Collectors.joining());
	}

}
