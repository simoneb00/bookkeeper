import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import utils.BookKeeperClusterTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestCreateLedger extends BookKeeperClusterTestCase {

    private int ensSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private BookKeeper.DigestType digestType;
    private byte[] passwd;


    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                {4, 3, 1, BookKeeper.DigestType.CRC32, "password".getBytes()},
                {4, 2, 1, BookKeeper.DigestType.MAC, "password".getBytes()}
        });
    }

    public TestCreateLedger(int ensSize, int writeQuorumSize, int ackQuorumSize, BookKeeper.DigestType digestType, byte[] passwd) {
        super(5, 180);
        this.ensSize = ensSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.digestType = digestType;
        this.passwd = passwd;
    }

    @Test
    public void testCreateLedger() {
        try {
            baseConf.setJournalWriteData(true);
            baseClientConf.setUseV2WireProtocol(true);
            BookKeeper bookKeeperClient = new BookKeeper(baseClientConf);
            LedgerHandle ledgerHandle = bookKeeperClient.createLedger(this.ensSize, this.writeQuorumSize, this.ackQuorumSize, this.digestType, this.passwd);
            Assert.assertNotNull(ledgerHandle);

        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (BKException e) {
            throw new RuntimeException(e);
        }
    }
}
