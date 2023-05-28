import org.apache.bookkeeper.common.component.Lifecycle;
import org.junit.Assert;
import org.junit.Test;

public class LifecycleTest {

    @Test
    public void test() {
        Lifecycle lifecycle = new Lifecycle();
        boolean result = lifecycle.moveToStarted();
        Assert.assertTrue(result);
    }

    @Test
    public void test1() {
        Lifecycle lifecycle = new Lifecycle();
        boolean result = lifecycle.canMoveToStopped();
        Assert.assertFalse(result);
    }
}
