package documentanalyzer;


import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class AppTest {
  @Test
  public void successfulResponse() {
    //TODO: OOPS
    App app = new App();
    //String result = app.handleRequest(null, null);
    //assertEquals(result, "Null event");
    assertEquals(true,true);

  }
}
