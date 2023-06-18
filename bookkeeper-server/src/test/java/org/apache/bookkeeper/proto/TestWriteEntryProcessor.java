package org.apache.bookkeeper.proto;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;

import static org.apache.bookkeeper.proto.BookieProtocol.*;

import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Before;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.*;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.apache.bookkeeper.proto.ProtoUtils.getMockedHandler;
import static org.apache.bookkeeper.proto.ProtoUtils.getMockedProcessor;
import static org.apache.bookkeeper.proto.ProtoUtils.getMockedRequest;
import static org.apache.bookkeeper.proto.ProtoUtils.VALID;
import static org.apache.bookkeeper.proto.ProtoUtils.INVALID;
import static org.apache.bookkeeper.proto.ProtoUtils.NULL;

/**
 * Unit test for the class {@link WriteEntryProcessor}
 */
@RunWith(MockitoJUnitRunner.class)
public class TestWriteEntryProcessor {

    private static Stream<Arguments> createWEPParams() {
        return Stream.of(
                // ParsedAddRequest instance, BookieRequestHandler instance, BookieRequestProcessor instance, exception expected
                Arguments.of(getMockedRequest(VALID), getMockedHandler(VALID), getMockedProcessor(VALID), false),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(INVALID), getMockedProcessor(VALID), true),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(VALID), getMockedProcessor(INVALID), true),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(VALID), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(NULL), getMockedProcessor(VALID), true),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(INVALID), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(NULL), getMockedProcessor(INVALID), true),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(NULL), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(INVALID), getMockedProcessor(INVALID), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(INVALID), getMockedProcessor(INVALID), true),
                //Arguments.of(getMockedRequest(INVALID), getMockedHandler(VALID), getMockedProcessor(VALID), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(VALID), getMockedProcessor(INVALID), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(INVALID), getMockedProcessor(VALID), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(VALID), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(NULL), getMockedProcessor(VALID), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(NULL), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(INVALID), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(NULL), getMockedProcessor(INVALID), true),
                Arguments.of(getMockedRequest(NULL), getMockedHandler(NULL), getMockedProcessor(NULL), true),
                //Arguments.of(getMockedRequest(NULL), getMockedHandler(VALID), getMockedProcessor(VALID), true),
                Arguments.of(getMockedRequest(NULL), getMockedHandler(VALID), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(NULL), getMockedHandler(NULL), getMockedProcessor(VALID), true),
                Arguments.of(getMockedRequest(NULL), getMockedHandler(VALID), getMockedProcessor(INVALID), true),
                Arguments.of(getMockedRequest(NULL), getMockedHandler(INVALID), getMockedProcessor(VALID), true),
                Arguments.of(getMockedRequest(NULL), getMockedHandler(INVALID), getMockedProcessor(INVALID), true),
                Arguments.of(getMockedRequest(NULL), getMockedHandler(NULL), getMockedProcessor(INVALID), true),
                Arguments.of(getMockedRequest(NULL), getMockedHandler(INVALID), getMockedProcessor(NULL), true)
        );
    }

    @ParameterizedTest
    @MethodSource("createWEPParams")
    public void testCreateWEP(ParsedAddRequest request, BookieRequestHandler handler, BookieRequestProcessor processor, boolean exceptionExpected) {
        try {
            WriteEntryProcessor p = WriteEntryProcessor.create(request, handler, processor);
            Assertions.assertNotNull(p);
            Assertions.assertFalse(exceptionExpected);

        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
            Assertions.assertTrue(exceptionExpected);
        }
    }

    private static Stream<Arguments> processOnROBookieParams() {
        return Stream.of(
                Arguments.of(false, false),
                Arguments.of(false, true),
                Arguments.of(true, false)
        );
    }


    /**
     * This method tests:
     * <p> <i>protected void processPacket()</i> </p>
     * in the case of read-only bookie. In particular, we expect back an error message showing the specific code for "add attempt on read-only bookie" (i.e. EREADONLY exit code).
     * We test the cases of:
     * <p>high priority write when the processor is not available for high priority writes</p>
     * <p>low priority write when the processor is available for high priority writes</p>
     * <p>low priority write when the processor is not available for high priority writes</p>
     * In the three cases, the expected result is always EREADONLY response.
     */
    @ParameterizedTest
    @MethodSource("processOnROBookieParams")
    public void testProcessPacketWithReadOnlyBookie(boolean isHighPriority, boolean isAvailableForHighPriorityWrite) {

        ParsedAddRequest request = getMockedRequest(VALID);

        Channel channel = mock(Channel.class);
        when(channel.isOpen()).thenReturn(true);

        BookieRequestHandler handler = mock(BookieRequestHandler.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        when(handler.ctx()).thenReturn(ctx);

        BookieImpl bookie = mock(BookieImpl.class);

        BookieRequestProcessor processor = mock(BookieRequestProcessor.class);
        when(processor.getBookie()).thenReturn(bookie);
        when(processor.getRequestStats()).thenReturn(new RequestStats(NullStatsLogger.INSTANCE));
        when(channel.isActive()).thenReturn(true);
        when(channel.isWritable()).thenReturn(true);

        /* we test that, even if the request has high priority, it cannot be executed if the processor is not available for high priority writes */
        when(request.isHighPriority()).thenReturn(isHighPriority);
        when(processor.getBookie().isAvailableForHighPriorityWrites()).thenReturn(isAvailableForHighPriorityWrite);

        WriteEntryProcessor wep = WriteEntryProcessor.create(
                request,
                handler,
                processor);

        when(bookie.isReadOnly()).thenReturn(true);
        ChannelPromise mockPromise = mock(ChannelPromise.class);
        when(channel.newPromise()).thenReturn(mockPromise);
        when(mockPromise.addListener(any())).thenReturn(mockPromise);

        AtomicReference<Object> writtenObject = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            writtenObject.set(invocationOnMock.getArgument(0));
            latch.countDown();
            return null;
        }).when(channel).writeAndFlush(any(), any());

        wep.processPacket();

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assertions.assertTrue(writtenObject.get() instanceof Response);
        Response response = (Response) writtenObject.get();
        Assertions.assertEquals(BookieProtocol.EREADONLY, response.getErrorCode());


    }

    /**
     * This method tests:
     * <p> <i>protected void processPacket()</i> </p>
     * in the case of not read-only bookie. In particular:
     * <p> if the WriteEntryProcessor is well configured and the bookie is not read-only, the write is successful </p>
     * Other test cases will be considered in the integration test with the class BookieImpl.java.
     */
    @Test
    public void testProcessPacket() {
        ParsedAddRequest request = getMockedRequest(VALID);
        BookieRequestHandler handler = getMockedHandler(VALID);

        BookieImpl bookie = mock(BookieImpl.class);

        BookieRequestProcessor processor = mock(BookieRequestProcessor.class);
        when(processor.getBookie()).thenReturn(bookie);

        WriteEntryProcessor wep = WriteEntryProcessor.create(request, handler, processor);

        /* we test the case in which the destination bookie is not read-only */
        when(bookie.isReadOnly()).thenReturn(false);

        boolean[] successfulWrite = {false};

        /* The following code defines a custom behavior for the method addEntry of the mocked bookie, in order to simulate a successful adding of the entry.
        *  In particular, if the method does not throw exceptions, we assume that the entry is successfully added. */
        try {
            doAnswer(invocationOnMock -> {
                successfulWrite[0] = true;
                return null;
            }).when(bookie).addEntry(any(), anyBoolean(), any(), any(), any());

            wep.processPacket();

        } catch (InterruptedException | IOException | BookieException e) {
            Assertions.fail("Unexpected exception");
        }

        Assertions.assertTrue(successfulWrite[0]);

        /* testing the case of a recovery add */
        when(request.isRecoveryAdd()).thenReturn(true);

        boolean[] successfulRecoveryWrite = {false};

        try {
            doAnswer(invocationOnMock -> {
                successfulRecoveryWrite[0] = true;
                return null;
            }).when(bookie).recoveryAddEntry(any(), any(), any(), any());

            wep.processPacket();

        } catch (InterruptedException | IOException | BookieException e) {
            Assertions.fail("Unexpected exception");
        }

        Assertions.assertTrue(successfulRecoveryWrite[0]);
    }

    /**
     * This method has been introduced after evaluating the coverage. It tests that a high priority request
     * is processed even on a read-only bookie, if the processor is available for high priority requests.
     */
    @Test
    public void testProcessHighPriorityRequestOnReadOnlyBookie() {
        ParsedAddRequest request = getMockedRequest(VALID);

        Channel channel = mock(Channel.class);
        when(channel.isOpen()).thenReturn(true);

        BookieRequestHandler handler = mock(BookieRequestHandler.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(ctx.channel()).thenReturn(channel);
        when(handler.ctx()).thenReturn(ctx);

        BookieImpl bookie = mock(BookieImpl.class);

        BookieRequestProcessor processor = mock(BookieRequestProcessor.class);
        when(processor.getBookie()).thenReturn(bookie);
        when(processor.getRequestStats()).thenReturn(new RequestStats(NullStatsLogger.INSTANCE));
        when(channel.isActive()).thenReturn(true);
        when(channel.isWritable()).thenReturn(true);

        when(request.isHighPriority()).thenReturn(true);
        when(processor.getBookie().isAvailableForHighPriorityWrites()).thenReturn(true);

        WriteEntryProcessor wep = WriteEntryProcessor.create(
                request,
                handler,
                processor);

        when(bookie.isReadOnly()).thenReturn(true);

        boolean[] successfulWrite = {false};

        /* The following code defines a custom behavior for the method addEntry of the mocked bookie, in order to simulate a successful adding of the entry.
         *  In particular, if the method does not throw exceptions, we assume that the entry is successfully added. */
        try {
            doAnswer(invocationOnMock -> {
                successfulWrite[0] = true;
                return null;
            }).when(bookie).addEntry(any(), anyBoolean(), any(), any(), any());

            wep.processPacket();

        } catch (InterruptedException | IOException | BookieException e) {
            Assertions.fail("Unexpected exception");
        }

        Assertions.assertTrue(successfulWrite[0]);

        /* testing the case of a recovery add */
        when(request.isRecoveryAdd()).thenReturn(true);

        boolean[] successfulRecoveryWrite = {false};

        try {
            doAnswer(invocationOnMock -> {
                successfulRecoveryWrite[0] = true;
                return null;
            }).when(bookie).recoveryAddEntry(any(), any(), any(), any());

            wep.processPacket();

        } catch (InterruptedException | IOException | BookieException e) {
            Assertions.fail("Unexpected exception");
        }

        Assertions.assertTrue(successfulRecoveryWrite[0]);
    }

}
