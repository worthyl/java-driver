package com.datastax.driver.core;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;

/**
 * This class uses subclasses for each type of partitioner.
 *
 * There's normally a way to parametrize a TestNG class with @Factory and @DataProvider,
 * but it doesn't seem to work with multiple methods.
 */
public abstract class MetadataTokenTest {

    List<String> schema = Lists.newArrayList(
        "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        "USE test",
        "CREATE TABLE IF NOT EXISTS foo(i int primary key)",
        "INSERT INTO foo (i) VALUES (1)",
        "INSERT INTO foo (i) VALUES (2)",
        "INSERT INTO foo (i) VALUES (3)"
    );

    private final String ccmOptions;
    private final DataType expectedTokenType;
    CCMBridge ccm;
    Cluster cluster;
    Session session;

    public MetadataTokenTest(String ccmOptions, DataType expectedTokenType) {
        this.ccmOptions = ccmOptions;
        this.expectedTokenType = expectedTokenType;
    }

    @BeforeClass(groups = "short")
    public void setup() {
        ccm = CCMBridge.create("test", 3, ccmOptions);
        cluster = Cluster.builder()
            .addContactPoints(CCMBridge.ipOfNode(1))
            .build();
        cluster.init();
        session = cluster.connect();
    }

    @AfterClass(groups = "short")
    public void teardown() {
        if (cluster != null)
            cluster.close();
        if (ccm != null)
            ccm.remove();
    }

    @Test(groups = "short")
    public void should_expose_token_ranges() throws Exception {
        Metadata metadata = cluster.getMetadata();

        for (String statement : schema)
            session.execute(statement);

        // Find the replica for a given partition key
        int testKey = 1;
        Set<Host> replicas = metadata.getReplicas("test", DataType.cint().serialize(testKey));
        assertThat(replicas).hasSize(1);
        Host replica = replicas.iterator().next();

        // Iterate the cluster's token ranges. For each one, use a range query to ask Cassandra which partition keys
        // are in this range.
        Set<TokenRange> ranges = metadata.getTokenRanges();
        TokenRange foundRange = null;
        for (TokenRange range : ranges) {
            List<Row> rows = Lists.newArrayList();
            for (TokenRange subRange : range.unwrap()) {
                rows.addAll(session.execute("SELECT i FROM foo WHERE token(i) > ? and token(i) <= ?",
                    subRange.getStart(), subRange.getEnd())
                    .all());
            }
            for (Row row : rows) {
                if (row.getInt("i") == testKey) {
                    // We should find our test key exactly once
                    assertThat(foundRange)
                        .describedAs("found the same key in two ranges: " + foundRange + " and " + range)
                        .isNull();
                    foundRange = range;
                    // That range should be managed by the replica
                    assertThat(metadata.getReplicas("test", range)).contains(replica);
                }
            }
        }
        assertThat(foundRange).isNotNull();
    }

    @Test(groups = "short")
    public void should_get_token_from_row_and_set_token_in_query() {
        Row row = session.execute("SELECT token(i) FROM foo WHERE i = 1").one();
        Token token = row.getToken(0);
        assertThat(token.getType()).isEqualTo(expectedTokenType);

        PreparedStatement pst = session.prepare("SELECT * FROM foo WHERE token(i) = ?");
        row = session.execute(pst.bind(token)).one();
        assertThat(row.getInt(0)).isEqualTo(1);

        row = session.execute(pst.bind().setToken(0, token)).one();
        assertThat(row.getInt(0)).isEqualTo(1);

        row = session.execute("SELECT * FROM foo WHERE token(i) = ?", token).one();
        assertThat(row.getInt(0)).isEqualTo(1);
    }
}
