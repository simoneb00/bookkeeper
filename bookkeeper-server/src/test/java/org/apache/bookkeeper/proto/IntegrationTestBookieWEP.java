package org.apache.bookkeeper.proto;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.bookkeeper.bookie.*;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
public class IntegrationTestBookieWEP {

    private static BookieImpl bookie;
    private static WriteEntryProcessor wep;
    private static ParsedAddRequest request;
    private static BookieRequestHandler handler;
    private static BookieRequestProcessor processor;

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
            when(request.getEntryId()).thenReturn(12345L);
            when(request.getLedgerId()).thenReturn(54321L);
            when(request.getMasterKey()).thenReturn("key".getBytes());
            when(request.getData()).thenReturn(Unpooled.copiedBuffer("testing-data", StandardCharsets.UTF_8));
            when(request.getOpCode()).thenReturn(BookieProtocol.ADDENTRY);

            Channel channel = mock(Channel.class);
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

    /**
     * This method tests the basic interactions between WriteEntryProcessor and BookieImpl. We consider the following test cases (the case of read-only bookies is covered on WEP's unit test class):
     * <p> if the bookie is not read-only, but we attempt to write on a fenced ledger, the add fails, and the fail is correctly propagated to the WEP </p>
     * <p> if the bookie is not read-only and the ledger is not fenced, the add is successful, and the success message is propagated to the WEP </p>
     *
     */
    @Test
    public void testWriteOnReadOnlyBookie() {
        //TODO implement
    }
}
