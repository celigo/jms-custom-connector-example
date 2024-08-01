package com.celigo.connectors.jms;

import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.json.JSONException;
import org.json.JSONObject;

import javax.jms.*;
import javax.jms.Queue;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class LambdaRequestHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        LambdaLogger logger = context.getLogger();

        logger.log("ClientContext: " + context.getClientContext().getCustom());
        logger.log("Input: " + input);

        String type = context.getClientContext().getCustom().get("type");
        String function = context.getClientContext().getCustom().get("function");

        if (!"wrapper".equals(type)) {
            return createErrorResponse("Invalid type: " + type, 422);
        }

        if (function == null || function.isEmpty()) {
            return createErrorResponse("Invalid function: " + function, 422);
        }

        BasicAWSCredentials awsCredentials;
        try {
            awsCredentials = extractCredentials(input, logger);
        } catch (Exception e) {
            return createErrorResponse(e.getMessage(), 401);
        }

        switch (function) {
            case "pingConnection":
                return pingConnection(awsCredentials, logger);
            case "consumeMessages":
                String maxResponseSizeStr = context.getClientContext().getCustom().get("maxResponseSize");
                int maxResponseSize;
                try {
                    maxResponseSize = Integer.parseInt(maxResponseSizeStr);
                    if (maxResponseSize <= 1024) {
                        return createErrorResponse("maxResponseSize should be greater than 1KB", 422);
                    }
                } catch (NumberFormatException e) {
                    return createErrorResponse("Invalid maxResponseSize: " + maxResponseSizeStr, 422);
                }
                return consumeMessages(awsCredentials, input, logger, maxResponseSize);
            case "sendMessages":
                return sendMessages(awsCredentials, input, logger);
            default:
                return createErrorResponse("No such function exists: " + function, 422);
        }
    }

    private Map<String, Object> pingConnection(BasicAWSCredentials awsCredentials, LambdaLogger logger) {
        Map<String, Object> response = new HashMap<>();
        try {
            AmazonSQS sqs = AmazonSQSClientBuilder.standard()
                    .withRegion(Regions.US_EAST_1)
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .build();
            sqs.listQueues(); // Test if we can list queues
            response.put("statusCode", 200);
        } catch (Exception e) {
            return createErrorResponse(e.getMessage(), 401);
        }
        return response;
    }

    private Map<String, Object> consumeMessages(BasicAWSCredentials awsCredentials, Map<String, Object> input, LambdaLogger logger, int maxResponseSize) {
        List<Map<String, Object>> data = new ArrayList<>();
        boolean lastPage = false;
        int currentSize = 0;

        try {
            String queueName = (String) ((Map<String, Object>) input.get("configuration")).get("queueName");
            logger.log("Queue Name: " + queueName);

            SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                    new ProviderConfiguration(),
                    AmazonSQSClientBuilder.standard()
                            .withRegion(Regions.US_EAST_1)
                            .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
            );

            javax.jms.Connection connection = connectionFactory.createConnection();
            connection.start();
            logger.log("JMS connection started");

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);
            MessageConsumer consumer = session.createConsumer(queue);
            logger.log("Message consumer created");

            while (currentSize < maxResponseSize) {
                Message message = consumer.receive(1000);
                logger.log("Received message: " + message);

                if (message == null) {
                    logger.log("No more messages, exiting loop");
                    lastPage = true;
                    break;
                }

                if (message instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) message;
                    String text = textMessage.getText();
                    logger.log("TextMessage received: " + text);
                    int messageSize = text.getBytes(StandardCharsets.UTF_8).length;

                    if (currentSize + messageSize > maxResponseSize) {
                        logger.log("Max response size exceeded, exiting loop");
                        break;
                    }

                    try {
                        JSONObject json = new JSONObject(text);
                        data.add(json.toMap());
                    } catch (JSONException e) {
                        Map<String, Object> json = new HashMap<>();
                        json.put("textMessage", text);
                        data.add(json);
                    }

                    currentSize += messageSize;
                } else if (message instanceof MapMessage) {
                    MapMessage mapMessage = (MapMessage) message;
                    Map<String, Object> json = new HashMap<>();
                    Enumeration<?> mapNames = mapMessage.getMapNames();
                    while (mapNames.hasMoreElements()) {
                        String key = (String) mapNames.nextElement();
                        json.put(key, mapMessage.getObject(key));
                    }
                    logger.log("MapMessage received: " + json);
                    data.add(json);
                } else if (message instanceof ObjectMessage) {
                    ObjectMessage objectMessage = (ObjectMessage) message;
                    Object object = objectMessage.getObject();

                    Map<String, Object> json = new HashMap<>();
                    if (object instanceof byte[]) {
                        try (ByteArrayInputStream bis = new ByteArrayInputStream((byte[]) object);
                             ObjectInputStream ois = new ObjectInputStream(bis)) {
                            Object obj = ois.readObject();
                            json = new ObjectMapper().convertValue(obj, Map.class);
                        } catch (Exception e) {
                            return createErrorResponse("Error deserializing ObjectMessage: " + e.getMessage(), 422);
                        }
                    } else {
                        json.put("objectMessage", object);
                    }
                    logger.log("ObjectMessage received: " + json);
                    data.add(json);
                }
            }

            consumer.close();
            session.close();
            connection.close();
            logger.log("JMS connection closed");

        } catch (JMSSecurityException e) {
            logger.log("JMSSecurityException: " + e.getMessage());
            return createErrorResponse(e.getMessage(), 401);
        } catch (JMSException e) {
            logger.log("JMSException: " + e.getMessage());
            return createErrorResponse(e.getMessage(), 422);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("data", data);
        response.put("lastPage", lastPage);
        logger.log("Response: " + response);
        return response;
    }

    private Map<String, Object> sendMessages(BasicAWSCredentials awsCredentials, Map<String, Object> input, LambdaLogger logger) {
        // Implement your sendMessages logic here
        // Use the extracted AWS credentials and queue name to send messages to the queue
        Map<String, Object> response = new HashMap<>();
        response.put("status", "sendMessages not implemented");
        return response;
    }

    private BasicAWSCredentials extractCredentials(Map<String, Object> input, LambdaLogger logger) throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, Object> connectionMap = (Map<String, Object>) input.get("connection");
        @SuppressWarnings("unchecked")
        Map<String, String> encrypted = (Map<String, String>) connectionMap.get("encrypted");
        @SuppressWarnings("unchecked")
        Map<String, String> unencrypted = (Map<String, String>) connectionMap.get("unencrypted");

        String awsAccessKeyId = unencrypted.get("AWS_ACCESS_KEY_ID");
        String awsSecretAccessKey = encrypted.get("AWS_SECRET_ACCESS_KEY");

        if (awsAccessKeyId == null || awsAccessKeyId.isEmpty()) {
            String errorMessage = "AWS_ACCESS_KEY_ID is missing. Please edit your connection resource in Celigo and set the following in the Unencrypted JSON field: {\"AWS_ACCESS_KEY_ID\":\"my_aws_access_key_id\"}";
            throw new Exception(errorMessage);
        }

        if (awsSecretAccessKey == null || awsSecretAccessKey.isEmpty()) {
            String errorMessage = "AWS_SECRET_ACCESS_KEY is missing. Please edit your connection resource in Celigo and set the following in the Encrypted JSON field: {\"AWS_SECRET_ACCESS_KEY\":\"my_aws_secret_access_key\"}";
            throw new Exception(errorMessage);
        }

        return new BasicAWSCredentials(awsAccessKeyId, awsSecretAccessKey);
    }

    private Map<String, Object> createErrorResponse(String errorMessage, int statusCode) {
        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", statusCode);
        Map<String, Object> error = new HashMap<>();
        error.put("message", errorMessage);
        List<Map<String, Object>> errors = new ArrayList<>();
        errors.add(error);
        response.put("errors", errors);
        return response;
    }
}
