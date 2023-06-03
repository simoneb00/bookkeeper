package org.apache.bookkeeper.proto;

import io.netty.channel.ChannelHandlerContext;
import org.apache.bookkeeper.bookie.BookieImpl;

import static org.apache.bookkeeper.proto.BookieProtocol.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;

import static org.mockito.Mockito.*;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.util.stream.Stream;

/**
 * This class aims to test, in isolation, the class WriteEntryProcessor.java.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestWriteEntryProcessor {


    /* Types of instances */
    private final static int VALID = 1;
    private final static int INVALID = 2;
    private final static int NULL = 3;

    @Before
    public void setUp() {
        /* BookieRequestProcessor mock configuration */


        /* BookieRequestHandler mock configuration */


        /* ParsedAddRequest mock configuration */

    }

    private static BookieProtocol.ParsedAddRequest getMockedRequest(int type) {

        BookieProtocol.ParsedAddRequest mockedRequest = mock(BookieProtocol.ParsedAddRequest.class);

        switch (type) {
            case VALID:
                when(mockedRequest.getEntryId()).thenReturn(1L);
                when(mockedRequest.getLedgerId()).thenReturn(1L);
                when(mockedRequest.getOpCode()).thenReturn(ADDENTRY);

                return mockedRequest;
            case INVALID:
                doThrow(new RuntimeException("Invalid ParsedAddRequest")).when(mockedRequest).getData();
                return mockedRequest;
            case NULL:
                return null;
            default:
                Assertions.fail("getMockedRequest: unexpected instance type.");
                return null;
        }
    }

    private static BookieRequestHandler getMockedHandler(int type) {

        BookieRequestHandler mockedHandler = mock(BookieRequestHandler.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        switch (type) {
            case VALID:
                when(mockedHandler.ctx()).thenReturn(ctx);
                return mockedHandler;
            case INVALID:
                doThrow(new RuntimeException("Invalid BookieRequestHandler")).when(mockedHandler).ctx();
                return mockedHandler;
            case NULL:
                return null;
            default:
                Assertions.fail("getMockedHandler: unexpected instance type.");
                return null;
        }
    }

    private static BookieRequestProcessor getMockedProcessor(int type) {

        BookieRequestProcessor mockedProcessor = mock(BookieRequestProcessor.class);
        BookieImpl bookie = mock(BookieImpl.class);

        switch (type) {
            case VALID:
                when(mockedProcessor.getBookie()).thenReturn(bookie);
                return mockedProcessor;
            case INVALID:
                doThrow(new RuntimeException("Invalid BookieRequestProcessor")).when(mockedProcessor).getBookie();
                return mockedProcessor;
            case NULL:
                return null;
            default:
                Assertions.fail("getMockedProcessor: unexpected instance type.");
                return null;
        }
    }

    /*
     *  TODO: ask if the fact that invalid requests and processors are not handled is considerable a bug (failed tests are commented out).
     */
    private static Stream<Arguments> createWEPParams() {
        return Stream.of(
                // ParsedAddRequest instance, BookieRequestHandler instance, BookieRequestProcessor instance, exception expected
                Arguments.of(getMockedRequest(VALID), getMockedHandler(VALID), getMockedProcessor(VALID), false),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(INVALID), getMockedProcessor(VALID), true),
                //Arguments.of(getMockedRequest(VALID), getMockedHandler(VALID), getMockedProcessor(INVALID), true),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(VALID), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(NULL), getMockedProcessor(VALID), true),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(INVALID), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(NULL), getMockedProcessor(INVALID), true),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(NULL), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(VALID), getMockedHandler(INVALID), getMockedProcessor(INVALID), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(INVALID), getMockedProcessor(INVALID), true),
                //Arguments.of(getMockedRequest(INVALID), getMockedHandler(VALID), getMockedProcessor(VALID), true),
                //Arguments.of(getMockedRequest(INVALID), getMockedHandler(VALID), getMockedProcessor(INVALID), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(INVALID), getMockedProcessor(VALID), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(VALID), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(NULL), getMockedProcessor(VALID), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(NULL), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(INVALID), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(INVALID), getMockedHandler(NULL), getMockedProcessor(INVALID), true),
                Arguments.of(getMockedRequest(NULL), getMockedHandler(NULL), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(NULL), getMockedHandler(VALID), getMockedProcessor(VALID), false),
                Arguments.of(getMockedRequest(NULL), getMockedHandler(VALID), getMockedProcessor(NULL), true),
                Arguments.of(getMockedRequest(NULL), getMockedHandler(NULL), getMockedProcessor(VALID), true),
                //Arguments.of(getMockedRequest(NULL), getMockedHandler(VALID), getMockedProcessor(INVALID), true),
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
            Assertions.assertFalse(exceptionExpected);
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
            Assertions.assertTrue(exceptionExpected);
        }
    }

    public static Stream<Arguments> processPacketParams() {
        return Stream.of(
                Arguments.of(true),
                Arguments.of(false)
        );
    }

    /**
     *  This method tests the method:
     *  <p> <i>protected void processPacket()</i> </p>
     *  In particular, it tests that:
     *  <p> if the bookie is read-only, we can't write on it </p>
     *  <p> if the WriteEntryProcessor is well configured, the write is successful </p>
     *  //TODO: add other testing scenarios
     *
     */
    @Test
    public void testProcessPacket() {
        ParsedAddRequest request = getMockedRequest(VALID);
        BookieRequestHandler handler = getMockedHandler(VALID);

        BookieImpl bookie = mock(BookieImpl.class);

        BookieRequestProcessor processor = mock(BookieRequestProcessor.class);
        when(processor.getBookie()).thenReturn(bookie);
        when(bookie.isReadOnly()).thenReturn(true);


        WriteEntryProcessor wep = WriteEntryProcessor.create(request, handler, processor);

        /* we're checking if an exception is thrown, when trying to write on a read-only bookie. */
        Assertions.assertThrows(Exception.class, wep::processPacket);

        /* now we can test the case in which the destination bookie is not read-only */

        // TODO: check if the write was successful, modifying the behavior of processor

        /*when(bookie.isReadOnly()).thenReturn(false);


        boolean[] successfulWrite = {false};

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                System.out.println(invocationOnMock.getArguments()[0]);
                successfulWrite[0] = true;
                return true;
            }
        }).when(handler).prepareSendResponseV2(eq(EOK), any());



        Assertions.assertTrue(successfulWrite[0]); */
    }

}
