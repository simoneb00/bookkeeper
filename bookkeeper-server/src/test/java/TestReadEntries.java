import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerHandleAdv;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.logging.log4j.core.tools.picocli.CommandLine;
import org.checkerframework.common.value.qual.ArrayLen;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import utils.BookKeeperClusterTestCase;

import java.awt.print.Book;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@RunWith(Parameterized.class)
public class TestReadEntries extends BookKeeperClusterTestCase {

    private long firstEntry;
    private long lastEntry;
    private boolean exceptionExpected;
    private BookKeeper bookKeeper;
    private LedgerHandle ledgerHandle;
    private LedgerHandleAdv ledgerHandleAdv;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                //firstEntry, lastEntry, exceptionExpected
                {-1, -2, true},     // expected: exception
                {0, 1, false}    // expected: entries with id 0 and 1 successfully read
        });
    }

    public TestReadEntries(long firstEntry, long lastEntry, boolean exceptionExpected) {
        super(5, 180);
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
        this.exceptionExpected = exceptionExpected;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        baseConf.setJournalWriteData(true);
        baseClientConf.setUseV2WireProtocol(true);
        this.bookKeeper = new BookKeeper(baseClientConf);

        // the focus of this test case is not to check if the method createLedger has bugs, we only need a ledgerHandle instance.
        this.ledgerHandle = this.bookKeeper.createLedgerAdv(4, 3, 2, BookKeeper.DigestType.CRC32, "password".getBytes());
        this.ledgerHandleAdv = (LedgerHandleAdv) ledgerHandle;

        // the method addEntry of LedgerHandleAdv has already been tested, so it's assumed to be reliable
        if (this.firstEntry >= 0 && this.lastEntry >= 0 && this.firstEntry <= this.lastEntry) {
            for (long id = this.firstEntry; id <= this.lastEntry; id++) {
                ledgerHandleAdv.addEntry(id, "data".getBytes());
            }
        }

    }

    @Test
    public void readAsyncTest() {

        try {
            CompletableFuture<LedgerEntries> completableFuture = this.ledgerHandle.readAsync(this.firstEntry, this.lastEntry);
            LedgerEntries ledgerEntries = completableFuture.get();

            ArrayList<LedgerEntry> entries = new ArrayList<>();
            for (long id = this.firstEntry; id <= this.lastEntry; id++) {
                entries.add(ledgerEntries.getEntry(id));
            }

            Assert.assertEquals(entries.size(), (this.lastEntry - this.firstEntry + 1));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(this.exceptionExpected);
        }
    }
}
