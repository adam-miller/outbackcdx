package outbackcdx;

import org.junit.*;
import org.junit.rules.TemporaryFolder;

import outbackcdx.Web.Status;
import outbackcdx.auth.NullAuthorizer;

import java.io.*;
import java.util.Collections;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static outbackcdx.Web.Method.*;
import static outbackcdx.Web.Status.OK;


public class ReplicationFeaturesTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private static final String TWO_RECORDS =
            "- 20050614070159 http://nla.gov.au/ text/html 200 AKMCCEPOOWFMGGO5635HFZXGFRLRGWIX - 337023 NLA-AU-CRAWL-000-20050614070144-00003-crawling016.archive.org\n"
            + "- 20030614070159 http://example.com/ text/html 200 AKMCCEPOOWFMGGO5635HFZXGFRLRGWIX - - - 337023 NLA-AU-CRAWL-000-20050614070144-00003-crawling016.archive.org\n";

    private Webapp webapp;

    private DataStore manager;

    @Before
    public void setUp() throws IOException {
        File root = folder.newFolder();
        manager = new DataStore(root, 256, null, Long.MAX_VALUE, null);
        webapp = new Webapp(manager, false, Collections.emptyMap(), null, Collections.emptyMap(), 10000, new QueryConfig(), null, null);
    }

    @After
    public void tearDown() {
    }

    // tests for replication features:
    // ensure that write urls are disabled on secondary
    // ensure that we can retrieve a sequenceNumber on secondary
    // ensure that we can delete WALs on primary
    @Test
    public void testReadOnly() throws Exception {
        FeatureFlags.setSecondaryMode(true);
        // make a request to a write-able url
        // it should 401.
        POST("/test", "- 20050614070159 http://nla.gov.au/ text/html 200 AKMCCEPOOWFMGGO5635HFZXGFRLRGWIX - 337023 NLA-AU-CRAWL-000-20050614070144-00003-crawling016.archive.org\n- 20030614070159 http://example.com/ text/html 200 AKMCCEPOOWFMGGO5635HFZXGFRLRGWIX - - - 337023 NLA-AU-CRAWL-000-20050614070144-00003-crawling016.archive.org\n", Status.FORBIDDEN);
        FeatureFlags.setSecondaryMode(false);
    }

    @Test
    public void testChangePolling() throws Exception {
        POST("/src", "- 20050614070159 http://nla.gov.au/ text/html 200 AKMCCEPOOWFMGGO5635HFZXGFRLRGWIX - 337023 NLA-AU-CRAWL-000-20050614070144-00003-crawling016.archive.org\n- 20030614070159 http://example.com/ text/html 200 AKMCCEPOOWFMGGO5635HFZXGFRLRGWIX - - - 337023 NLA-AU-CRAWL-000-20050614070144-00003-crawling016.archive.org\n", OK);
        POST("/dest", "- 20050614070159 http://nla.gov.au/ text/html 200 AKMCCEPOOWFMGGO5635HFZXGFRLRGWIX - 337023 NLA-AU-CRAWL-000-20050614070144-00003-crawling016.archive.org\n- 20030614070159 http://example.com/ text/html 200 AKMCCEPOOWFMGGO5635HFZXGFRLRGWIX - - - 337023 NLA-AU-CRAWL-000-20050614070144-00003-crawling016.archive.org\n", OK);
        try (UWeb.UServer server = new UWeb.UServer("localhost", 0, "", webapp, new NullAuthorizer())) {
            server.start();
            ChangePollingThread pollingThread = new ChangePollingThread("http://localhost:" + server.port() + "/src", 1000, 10 * 1024 * 1024, manager);

            // Override the destination collection (by default it'll try to replicate src to itself)
            pollingThread.collection = "dest";
            pollingThread.index = manager.getIndex("dest", false);

            // Run an initial replication
            pollingThread.finalUrl = pollingThread.primaryReplicationUrl + "/changes?size=" + pollingThread.batchSize + "&since=" + 0;
            pollingThread.replicate();

            long initialSrcSeqNo = Long.parseLong(GET("/src/sequence", OK));

            // Now add a new record to the source collection
            POST("/src", "- 20050614070159 http://nla.gov.au/two text/html 200 AKMCCEPOOWFMGGO5635HFZXGFRLRGWIX - 337023 NLA-AU-CRAWL-000-20050614070144-00003-crawling016.archive.org\n", OK);

            long updatedSrcSeqNo = Long.parseLong(GET("/src/sequence", OK));
            assertTrue(updatedSrcSeqNo > initialSrcSeqNo);

            // Replicate again
            pollingThread.finalUrl = pollingThread.primaryReplicationUrl + "/changes?size=" + pollingThread.batchSize + "&since=" + initialSrcSeqNo;
            pollingThread.replicate();

            // We should see the new record now appears in the destination collection too
            String response = GET("/dest", OK, "url", "http://nla.gov.au/two");
            assertTrue(response.contains("http://nla.gov.au/two"));
        }
    }

    /**
     * The cursor must land on the primary's next unwritten sequence, so that a
     * caught-up replica transfers nothing. Storing the applied batch's own
     * sequence number left it one batch behind, and because /changes is
     * inclusive of `since` the replica then re-fetched and re-applied that same
     * batch on every poll indefinitely.
     *
     * The exact-value assertion is also what catches reading count() after the
     * marker put: that yields primary latest + 2.
     */
    @Test
    public void testCursorTracksPrimaryTail() throws Exception {
        POST("/src", TWO_RECORDS, OK);
        POST("/dest", "", OK);
        try (UWeb.UServer server = new UWeb.UServer("localhost", 0, "", webapp, new NullAuthorizer())) {
            server.start();
            ChangePollingThread polling = pollingThread(server, "dest");

            replicateSince(polling, 0);
            long primaryLatest = Long.parseLong(GET("/src/sequence", OK));
            assertEquals(primaryLatest + 1, storedCursor(polling));

            // Nothing new upstream: the poll must move no data and no cursor,
            // and must not raise -- the feed answers empty rather than 500.
            replicateSince(polling, storedCursor(polling));
            assertEquals(primaryLatest + 1, storedCursor(polling));
        }
    }

    /**
     * A cursor one sequence too high still finds a following batch of two or
     * more entries, because getUpdatesSince() returns the batch containing the
     * requested sequence. A batch of exactly one entry is skipped outright, so
     * only this case exposes that error -- as missing records rather than as a
     * failure at the point of the mistake.
     */
    @Test
    public void testSingleEntryBatchIsNotSkipped() throws Exception {
        POST("/src", TWO_RECORDS, OK);
        POST("/dest", "", OK);
        try (UWeb.UServer server = new UWeb.UServer("localhost", 0, "", webapp, new NullAuthorizer())) {
            server.start();
            ChangePollingThread polling = pollingThread(server, "dest");
            replicateSince(polling, 0);

            long before = Long.parseLong(GET("/src/sequence", OK));
            POST("/src", "- 20050614070159 http://nla.gov.au/single text/html 200 AKMCCEPOOWFMGGO5635HFZXGFRLRGWIX - 337023 NLA-AU-CRAWL-000-20050614070144-00003-crawling016.archive.org\n", OK);
            long after = Long.parseLong(GET("/src/sequence", OK));
            assertEquals("expected a single-entry batch", 1, after - before);

            replicateSince(polling, storedCursor(polling));
            assertTrue(GET("/dest", OK, "url", "http://nla.gov.au/single")
                    .contains("http://nla.gov.au/single"));
            assertEquals(after + 1, storedCursor(polling));
        }
    }

    /**
     * Asking for a sequence the primary has not written yet is the steady state
     * of a caught-up replica, so it answers with an empty feed. It used to be a
     * 500, which would have meant a logged exception on every idle poll.
     */
    @Test
    public void testChangeFeedBeyondTailIsEmptyNotError() throws Exception {
        POST("/src", TWO_RECORDS, OK);
        long latest = Long.parseLong(GET("/src/sequence", OK));
        assertEquals(Webapp.EMPTY_CHANGE_FEED,
                GET("/src/changes", OK, "since", String.valueOf(latest + 1)));
    }

    private ChangePollingThread pollingThread(UWeb.UServer server, String destination) throws IOException {
        ChangePollingThread polling = new ChangePollingThread(
                "http://localhost:" + server.port() + "/src", 1000, 10 * 1024 * 1024, manager);
        // By default it would replicate src to itself.
        polling.collection = destination;
        polling.index = manager.getIndex(destination, false);
        return polling;
    }

    private void replicateSince(ChangePollingThread polling, long since) throws Exception {
        polling.finalUrl = polling.primaryReplicationUrl
                + "/changes?size=" + polling.batchSize + "&since=" + since;
        polling.replicate();
    }

    private long storedCursor(ChangePollingThread polling) throws Exception {
        byte[] value = polling.index.db.get(polling.SEQ_NUM_KEY);
        assertNotNull("replication cursor was never stored", value);
        return Long.parseLong(new String(value, US_ASCII));
    }

    /**
     * A collection whose WAL holds no batches yields an unpositioned
     * TransactionLogIterator, and getBatch() must not be called on one.
     *
     * Without the isValid() guard in ChangeFeedJsonStream this fails here as an
     * AssertionError from RocksDB's own assert(isValid()), because surefire runs
     * with assertions enabled. Production does not: there the assert is skipped,
     * getBatch() returns a WriteBatch whose native handle is 0, and
     * WriteBatch.data() kills the JVM with a SIGSEGV.
     */
    @Test
    public void testChangeFeedOnCollectionWithNoWrites() throws Exception {
        // Creates the collection without writing any records to it.
        POST("/nowrites", "", OK);
        assertEquals("[\n\n]\n", GET("/nowrites/changes", OK, "since", "0"));
    }

    /*@Test
    public void testDeleteWals() throws Exception {
        FeatureFlags.setSecondaryMode(false);
        // post some CDX
        POST("/testb", "- 20050614070159 http://nla.gov.au/ text/html 200 AKMCCEPOOWFMGGO5635HFZXGFRLRGWIX - 337023 NLA-AU-CRAWL-000-20050614070144-00003-crawling016.archive.org\n- 20030614070159 http://example.com/ text/html 200 AKMCCEPOOWFMGGO5635HFZXGFRLRGWIX - - - 337023 NLA-AU-CRAWL-000-20050614070144-00003-crawling016.archive.org\n", OK);
        // check that we get items back from the iterator
        String output = GET("/testb/changes", OK, "since", "0");
        // assert output != {}
        assertNotEquals("{}", output);
        Index index = manager.getIndex("testb");
	index.db.flushWal(true);
        // make a request to delete WAL
        POST("/testb/truncate_replication", String.valueOf(index.db.getLatestSequenceNumber()), OK);
        // check that we get back no items from the iterator
        output = GET("/testb/changes", OK, "since", "0");
        assertEquals("{}", output);
        FeatureFlags.setSecondaryMode(false);
    }*/

    private String GET(String url, int expectedStatus) throws Exception {
        Web.Response response = webapp.handle(new DummyRequest(GET, url));
        assertEquals(expectedStatus, response.getStatus());
        return slurp(response);
    }

    private String GET(String url, int expectedStatus, String... parmKeysAndValues) throws Exception {
        DummyRequest request = new DummyRequest(GET, url);
        for (int i = 0; i < parmKeysAndValues.length; i += 2) {
            request.parm(parmKeysAndValues[i], parmKeysAndValues[i + 1]);
        }
        Web.Response response = webapp.handle(request);
        if (response == Web.Response.ALREADY_SENT) {
            response = request.streamedResponse();
        }
        assertEquals(expectedStatus, response.getStatus());
        return slurp(response);
    }

    private String DELETE(String url, int expectedStatus) throws Exception {
        Web.Response response = webapp.handle(new DummyRequest(DELETE, url));
        assertEquals(expectedStatus, response.getStatus());
        return slurp(response);
    }

    private String POST(String url, String data, int expectedStatus) throws Exception {
        Web.Response response = webapp.handle(new DummyRequest(POST, url, data));
        assertEquals(expectedStatus, response.getStatus());
        return slurp(response);
    }

    private String slurp(Web.Response response) throws IOException {
        Web.IStreamer streamer = response.getBodyWriter();
        if (streamer != null) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            streamer.stream(out);
            return out.toString(UTF_8);
        }
        return "";
    }

}
