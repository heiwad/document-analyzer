package helloworld;


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
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
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
    private static String TABLE_NAME;
    private static String TABLE_HASH;
    private static String TABLE_RANGE;

    public String handleRequest(S3Event s3event, Context context) {
            TABLE_NAME = System.getenv("TABLE_NAME");
            TABLE_HASH = System.getenv("TABLE_HASH");
            TABLE_RANGE = System.getenv("TABLE_RANGE");

      //  try {
            S3EventNotificationRecord record = s3event.getRecords().get(0);
            String srcBucket = record.getS3().getBucket().getName();

            // Object key may have spaces or unicode non-ASCII characters.
            String srcKey = record.getS3().getObject().getUrlDecodedKey();

            // Infer the image type from the file name.
            Matcher matcher = Pattern.compile(".*\\.([^\\.]*)").matcher(srcKey.toLowerCase());
            if (!matcher.matches()) {
                System.out.println("Unable to infer image type for key "
                        + srcKey);
                return "";
            }
            
            String imageType = matcher.group(1);
            if (!(JPG_TYPE.equals(imageType)) && !(PNG_TYPE.equals(imageType)) && !(PDF_TYPE.equals(imageType))) {
                System.out.println("Skipping non-image " + srcKey);
                return "";
            }

            System.out.println("Invoked with valid document: " + srcBucket + "/" + srcKey);

            System.out.println("Using textract to identify text");
            // Use Textract to process the document
            List<Block> blocks = extractText(srcBucket, srcKey);

            // Convert the text blocks into strings
            List<List<String>> processed = getText(blocks);
            List<String> ids = processed.get(0);
            List<String> text = processed.get(1);

            //Use Comprehend to process the text.
            System.out.println("Using comprehend to process entities");
            List<BatchDetectEntitiesItemResult> entities = getEntities(text);

            

            System.out.println(entities.toString());

            System.out.println("Saving analysis to dynamodb");
            writeDocumentToDynamoDB(ids, text, entities, srcKey);

        return srcKey;    
   //     } catch (IOException e) {
   //         throw new RuntimeException(e);
   //     }
    }

    private List<Block> extractText(String bucketName, String documentName) {

        S3Object document = new S3Object().withBucket(bucketName).withName(documentName);

        AmazonTextract client = AmazonTextractClientBuilder.defaultClient();
        DetectDocumentTextRequest request = new DetectDocumentTextRequest()
                .withDocument(new Document().withS3Object(document));

        DetectDocumentTextResult result = client.detectDocumentText(request);
        return result.getBlocks();

    }
    
    private List<List<String>> getText(List<Block> blocks) {
        System.out.println("Analyzing " + String.valueOf(blocks.size()) + " blocks of text");
        List<String> ids = new ArrayList<String>();
        List<String> text = new ArrayList<String>();

        blocks.forEach((block) -> {
            
            if ( (block.getBlockType() == "LINE") && !(block.getText() == null)){
            ids.add(block.getId());
            text.add(block.getText());
            System.out.println(block.getText());
            }
        });

        List<List<String>> processed = new ArrayList<List<String>>();
        processed.add(ids);
        processed.add(text);
        return processed;

    }

    private List<BatchDetectEntitiesItemResult> getEntities(List<String> text) {
        // Issue: Initializing client each time instead outside lambda handler| This is detected by CodeGuru
        AmazonComprehend comprehendClient = AmazonComprehendClientBuilder.standard().build();
        List<BatchDetectEntitiesItemResult> results = new ArrayList<BatchDetectEntitiesItemResult>();

        int current = 0;
        int BATCH_SIZE = 25;
        while (current < text.size() +1) {
            int step = Math.min(text.size() - current, BATCH_SIZE);
            List<String> batchText = text.subList(current, current + step);
            System.out.println("Submitting blocks to comprehend: " + String.valueOf(current) + " - " + String.valueOf(current + step));

            // ISSUE: Hard-coded the language to english. Jon should catch this.
            BatchDetectEntitiesRequest batchDetectEntitiesRequest = new BatchDetectEntitiesRequest().withTextList(batchText)
                .withLanguageCode("en");

            BatchDetectEntitiesResult batchDetectEntitiesResult = comprehendClient
                .batchDetectEntities(batchDetectEntitiesRequest);

            //ISSUE: Batch detect could have some failed entries but I didn't check and retry. This was caught in the DynamoDB sample but not here?
            results.addAll(batchDetectEntitiesResult.getResultList());

             current = current + step;

        }


        return results;

    }

    private static void writeDocumentToDynamoDB(List<String> ids, List<String> text,
            List<BatchDetectEntitiesItemResult> entities, String srcKey) {

        System.out.println("Size of ids: " + String.valueOf(ids.size()));
        System.out.println("Size of text: " + String.valueOf(text.size()));
        System.out.println("Size of entities: " + String.valueOf(entities.size()));

        AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(ddb);

        TableWriteItems tableWriteItems = new TableWriteItems(TABLE_NAME);
            
            //Looping Issue: Iterating over 3 lists but only checking the length of one of the lists!!
            for (int i = 0; i < ids.size(); i++) {
                
                //Build list of detected entities for each item
                List<Map<String,String>> itemEntityList = new ArrayList<Map<String,String>>();

                entities.get(i).getEntities().forEach(entity-> {
                    Map<String,String> itemEntity = new HashMap<String, String>();
                    
                    itemEntity.put("text",entity.getText());
                    itemEntity.put("type", entity.getType());
                    itemEntity.put("score", String.valueOf(entity.getScore()));

                    itemEntityList.add(itemEntity);

                });

                Item item = new Item()
                .withPrimaryKey(TABLE_HASH, srcKey, TABLE_RANGE, ids.get(i))
                .withString("text",text.get(i))
                .withList("entities",itemEntityList);
                
                tableWriteItems.addItemToPut(item);

            }

            BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(tableWriteItems);
            System.out.println(outcome.toString());
            //Did not check results of Batch Write - some of the writes could have failed. I need to rety.
            return;


    }



}


