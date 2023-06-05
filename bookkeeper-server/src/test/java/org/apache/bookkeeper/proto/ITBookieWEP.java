package org.apache.bookkeeper.proto;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.bookkeeper.bookie.*;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.bookkeeper.proto.BookieProtocol.ParsedAddRequest;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.*;

/**
 * This class tests the integration between the classes <i>BookieImpl</i> and <i>WriteEntryProcessor</i>, in particular between the methods:
 * <p> processPacket of WriteEntryProcessor </p>
 * <p> addEntry and recoveryAddEntry of BookieImpl </p>
 * This test assumes that the two classes have been unit tested.
 */
public class ITBookieWEP {

    private static BookieImpl bookie;
    private static WriteEntryProcessor wep;
    private static ParsedAddRequest request;
    private static BookieRequestHandler handler;
    private static BookieRequestProcessor processor;
    private static Channel channel;

    @BeforeEach
    public void setUp() {
        try {

            // Bookie configuration and startup
            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            conf.setJournalWriteData(false);    // this to ensure that cb is used

            bookie = new BookieSetUp(conf);
            bookie.start();


            request = mock(ParsedAddRequest.class);
            when(request.getProtocolVersion()).thenReturn(BookieProtocol.CURRENT_PROTOCOL_VERSION);
            when(request.getEntryId()).thenReturn(System.currentTimeMillis());
            when(request.getLedgerId()).thenReturn(System.currentTimeMillis()+1);
            when(request.getMasterKey()).thenReturn("key".getBytes());
            when(request.getData()).thenReturn(Unpooled.copiedBuffer("test" + System.currentTimeMillis() + 3, StandardCharsets.UTF_8));
            when(request.getOpCode()).thenReturn(BookieProtocol.ADDENTRY);

            channel = mock(Channel.class);
            when(channel.isOpen()).thenReturn(true);
            when(channel.isActive()).thenReturn(true);
            when(channel.isWritable()).thenReturn(true);

            handler = mock(BookieRequestHandler.class);
            ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
            when(ctx.channel()).thenReturn(channel);
            when(handler.ctx()).thenReturn(ctx);

            processor = mock(BookieRequestProcessor.class);
            when(processor.getBookie()).thenReturn(bookie);
            when(processor.getRequestStats()).thenReturn(new RequestStats(NullStatsLogger.INSTANCE));

            wep = WriteEntryProcessor.create(request, handler, processor);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method tests that the class BookieImpl is reachable from WriteEntryProcessor, when performing an entry add
     */
    @Test
    public void testReachabilityNormalAdd() {

        BookieImpl bookieSpy = spy(bookie);
        when(processor.getBookie()).thenReturn(bookieSpy);

        try {
            when(request.isRecoveryAdd()).thenReturn(false);
            wep.processPacket();
            verify(bookieSpy, times(1)).addEntry(any(), anyBoolean(), any(), any(), any());

        } catch (IOException | InterruptedException | BookieException e) {
            Assertions.fail("Unexpected exception in testing the reachability");
        }

    }


    /**
     * This method tests that the class BookieImpl is reachable from WriteEntryProcessor, when performing a recovery entry add
     */
    @Test
    public void testReachabilityRecoveryAdd() {

        BookieImpl bookieSpy = spy(bookie);
        when(processor.getBookie()).thenReturn(bookieSpy);

        try {
            when(request.isRecoveryAdd()).thenReturn(true);
            wep.processPacket();
            verify(bookieSpy, times(1)).recoveryAddEntry(any(), any(), any(), any());

        } catch (IOException | InterruptedException | BookieException e) {
            Assertions.fail("Unexpected exception in testing the reachability");
        }
    }



    /*
     * The following methods test the basic interactions between WriteEntryProcessor and BookieImpl. We consider the following test cases (the case of read-only bookies is covered on WEP's unit test class):
     * - if the bookie is not read-only, but we attempt to write on a fenced ledger, the add fails, and the fail is correctly propagated to the WEP
     * - if the bookie is not read-only and the ledger is not fenced:
     *      - if the request is a recovery add, then a recovery add must be executed (BookieImpl method recoveryAddEntry)
     *      - if the request is a normal add, then a normal add must be executed (BookieImpl method addEntry)
     */

    @Test
    public void addEntryOnFencedLedger() {

        when(request.getData()).thenReturn( Unpooled.copiedBuffer("testing_data_for_fenced_ledger", StandardCharsets.UTF_8));
        long ledgerId = request.getData().getLong(request.getData().readerIndex()); // this is the id of the ledger that will be created by BookieImpl method getLedgerForEntry

        try {
            bookie.fenceLedger(ledgerId, "key".getBytes());

        } catch (IOException | BookieException e) {
            Assertions.fail("Unexpected exception while fencing the ledger.");
        }

        AtomicReference<Object> reference = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        when(processor.getRequestStats()).thenReturn(new RequestStats(new NullStatsLogger()));

        doAnswer(invocationOnMock -> {
            reference.set(invocationOnMock.getArgument(0));
            latch.countDown();
            return null;
        }).when(channel).writeAndFlush(any(), any());

        wep.processPacket();

        try {
            latch.await();
        } catch (InterruptedException e) {
            Assertions.fail("Unexpected exception while waiting");
        }

        BookieProtocol.Response response = (BookieProtocol.Response) reference.get();
        Assertions.assertEquals(BookieProtocol.EFENCED, response.getErrorCode());
    }

    @Test
    public void testRecoveryAddEntry() {

        assert !bookie.isReadOnly();
        when(request.isRecoveryAdd()).thenReturn(true);

        AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        when(processor.getRequestStats()).thenReturn(new RequestStats(new NullStatsLogger()));

        doAnswer(invocationOnMock -> {
            atomicBoolean.set(true);
            latch.countDown();
            return null;
        }).when(handler).prepareSendResponseV2(eq(BookieProtocol.EOK), any());

        wep.processPacket();

        try {
            latch.await();
        } catch (InterruptedException e) {
            Assertions.fail("Unexpected exception while waiting");
        }

        Assertions.assertTrue(atomicBoolean.get());

    }

    @Test
    public void testAddEntry() {

        assert !bookie.isReadOnly();
        when(request.isRecoveryAdd()).thenReturn(false);

        AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        when(processor.getRequestStats()).thenReturn(new RequestStats(new NullStatsLogger()));

        doAnswer(invocationOnMock -> {
            atomicBoolean.set(true);
            latch.countDown();
            return null;
        }).when(handler).prepareSendResponseV2(eq(BookieProtocol.EOK), any());

        wep.processPacket();

        try {
            latch.await();
        } catch (InterruptedException e) {
            Assertions.fail("Unexpected exception while waiting");
        }

        Assertions.assertTrue(atomicBoolean.get());
    }
}
