package documentanalyzer;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;

import com.amazonaws.services.textract.model.S3Object;
import com.amazonaws.services.textract.AmazonTextract;
import com.amazonaws.services.textract.AmazonTextractClientBuilder;
import com.amazonaws.services.textract.model.*;

import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.AmazonComprehendClientBuilder;
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesItemResult;
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesRequest;
import com.amazonaws.services.comprehend.model.BatchDetectEntitiesResult;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;


/**
 * Handler for requests to Lambda function.
 */
public class App implements RequestHandler<S3Event, String> {

    private final String JPG_TYPE = (String) "jpg";
    private final String PNG_TYPE = (String) "png";
    private final String PDF_TYPE = (String) "pdf";
    private final String TABLE_NAME;
    private final String TABLE_HASH;
    private final String TABLE_RANGE;
    private final int MAX_TEXTRACT_BATCH_SIZE = 25;
    private final int MAX_DYNAMODB_BATCH_SIZE = 25;
    private final AmazonDynamoDB ddb;
    private final DynamoDB dynamoDB;

    private final AmazonTextract textract;

    public App() {

        textract = AmazonTextractClientBuilder.defaultClient();

        // Set up connection to dynamoDB
        TABLE_NAME = System.getenv("TABLE_NAME");
        TABLE_HASH = System.getenv("TABLE_HASH");
        TABLE_RANGE = System.getenv("TABLE_RANGE");
        ddb = AmazonDynamoDBClientBuilder.defaultClient();
        dynamoDB = new DynamoDB(ddb);

    }

    public String handleRequest(final S3Event s3event, final Context context) {

        final S3EventNotificationRecord record = s3event.getRecords().get(0);
        final String srcBucket = record.getS3().getBucket().getName();
        // Object key may have spaces or unicode non-ASCII characters.
        final String srcKey = record.getS3().getObject().getUrlDecodedKey();

        // Infer the image type from the file name.
        final Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(srcKey.toLowerCase());
        if (!matcher.matches()) {
            System.out.println("Unable to infer image type for key " + srcKey);
            return "";
        }

        final String imageType = matcher.group(1);
        if (!(JPG_TYPE.equals(imageType)) && !(PNG_TYPE.equals(imageType)) && !(PDF_TYPE.equals(imageType))) {
            System.out.println("Skipping non-image " + srcKey);
            return "";
        }

        System.out.println("Invoked with valid document: " + srcBucket + "/" + srcKey);

        processDocument(srcBucket, srcKey);

        return srcKey;

    }

    private void processDocument(final String srcBucket, final String srcKey) {

        // Use Textract to process the document
        final List<Block> blocks = extractText(srcBucket, srcKey);

        // Convert the blocks for lines of text into strings
        final List<String> ids = new ArrayList<String>();
        final List<String> text = new ArrayList<String>();
        blocks.forEach(block -> {
            ids.add(block.getId());
            text.add(block.getText());
        });

        // Use Comprehend to process the text.
        final List<BatchDetectEntitiesItemResult> entities = getEntities(text);

        System.out.println("Saving analysis to dynamodb");
        writeDocumentToDynamoDB(ids, text, entities, srcKey);

    }

    private List<Block> extractText(final String bucketName, final String documentName) {
        System.out.println("Using textract to identify text in the document.");

        final S3Object document = new S3Object().withBucket(bucketName).withName(documentName);

        final DetectDocumentTextRequest request = new DetectDocumentTextRequest()
                .withDocument(new Document().withS3Object(document));

        final DetectDocumentTextResult result = textract.detectDocumentText(request);

        // Keep the non-empty lines of text and ignore the individual words
        final List<Block> filteredResults = new ArrayList<Block>();
        result.getBlocks().forEach(block -> {
            if (block.getBlockType().equals("LINE") && !(block.getText() == null)) {
                filteredResults.add(block);
            }
        });

        return filteredResults;

    }

    private List<BatchDetectEntitiesItemResult> getEntities(final List<String> text) {

        System.out.println("Using comprehend to process " + String.valueOf(text.size()) + " entities.");

        final AmazonComprehend comprehendClient = AmazonComprehendClientBuilder.standard().build();
        final List<BatchDetectEntitiesItemResult> results = new ArrayList<BatchDetectEntitiesItemResult>();

        // Detect Entities for each line of text in batches of up to 25
        for (int current = 0; current < text.size();) {

            final int batchSize = Math.min(text.size() - current, MAX_TEXTRACT_BATCH_SIZE);
            final List<String> batch = text.subList(current, current + batchSize);

            final BatchDetectEntitiesRequest batchDetectEntitiesRequest = new BatchDetectEntitiesRequest()
                    .withTextList(batch)
                    .withLanguageCode("en");

            final BatchDetectEntitiesResult batchDetectEntitiesResult = comprehendClient
                    .batchDetectEntities(batchDetectEntitiesRequest);

            results.addAll(batchDetectEntitiesResult.getResultList());
            current = current + batchSize;

        }
        System.out.println("Processed entities");
        System.out.println(results);
        return results;

    }

    private void writeDocumentToDynamoDB(final List<String> ids, final List<String> text,
            final List<BatchDetectEntitiesItemResult> entities, final String srcKey) {

        // Build up item objects for the current document
        final List<Item> items = new ArrayList<Item>();
        for (int i = 0; i < ids.size(); i++) {

            // Build list of detected entities for each item
            final List<Map<String, String>> itemEntityList = new ArrayList<Map<String, String>>();

            entities.get(i).getEntities().forEach(entity -> {
                final Map<String, String> itemEntity = new HashMap<String, String>();

                itemEntity.put("text", entity.getText());
                itemEntity.put("type", entity.getType());
                itemEntity.put("score", String.valueOf(entity.getScore()));
                itemEntityList.add(itemEntity);

            });

            Item item; 
            if (itemEntityList.size() == 0) {
               item = new Item().withPrimaryKey(TABLE_HASH, srcKey, TABLE_RANGE, ids.get(i))
                    .withString("text", text.get(i));
            } else {
                item = new Item().withPrimaryKey(TABLE_HASH, srcKey, TABLE_RANGE, ids.get(i))
                    .withString("text", text.get(i)).withList("entities", itemEntityList);
            }

            items.add(item);

        }

        // Write items to DynamoDB using batches of up to 100
        for (int current = 0; current < ids.size();) {

            final int batchSize = Math.min(ids.size() - current, MAX_DYNAMODB_BATCH_SIZE);
            final List<Item> batch = items.subList(current, current + batchSize);
            final TableWriteItems tableWriteItems = new TableWriteItems(TABLE_NAME);
            
            batch.forEach(item->{
                tableWriteItems.addItemToPut(item);
            });
            
            dynamoDB.batchWriteItem(tableWriteItems);
            current = current + batchSize;

        }

        return;

    }

}


