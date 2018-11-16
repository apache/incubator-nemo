import org.apache.commons.lang.SerializationUtils;
import org.apache.nemo.runtime.lambda.SerializeUtils;
import org.junit.Test;

public class SimpleTest {

  @Test
  public void testSerialize() {
    final Query7SideInputHandler obj = new Query7SideInputHandler();
    System.out.println(SerializeUtils.serializeToString(obj));
  }
}
