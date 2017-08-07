import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Token;
import com.google.common.collect.Lists;
import com.instaclustr.tokenator.Tokenator;
import org.junit.Test;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class TokenatorTests {

    private static final int DEFAULT_PORT = 9042;
    private static final InetAddress IPV4_LOOPBACK = Inet4Address.getLoopbackAddress();

    @Test
    public void testAllocations() {
        boolean sslEnabled = false;
        InetAddress address = IPV4_LOOPBACK;
        int port = DEFAULT_PORT;
        AuthProvider authProvider = null;
        try (final Cluster cluster = Tokenator.connect(address, port, authProvider, sslEnabled)) {
            final Metadata metadata = cluster.getMetadata();

            int numTokensNode = 256;
            long minimum = Long.MIN_VALUE;
            long maximum = Long.MAX_VALUE;

            int total_tokens = numTokensNode * 6;

            //Currently this test requires a running C* cluster as all the token stuff is package local...


            Map<Token, Set<Token>> sortedTokens = IntStream.range(0, total_tokens)
                    .boxed()
                    .map(i -> {
                        Token primary = metadata.newToken(String.valueOf(((maximum / total_tokens) * i) - minimum));
                        return new AbstractMap.SimpleEntry<>(primary, IntStream.range(0, 3)
                                .boxed()
                                .map(j -> metadata.newToken(String.valueOf(((maximum / total_tokens) * (i + j)) - minimum)))
                                .collect(Collectors.toSet()));
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            List<Token> sortPri = Lists.newArrayList(sortedTokens.keySet());
            Collections.sort(sortPri);

            HashSet<com.datastax.driver.core.TokenRange> candidateTokensTest = Tokenator.generateCandidates(sortedTokens, sortPri, metadata);
            assertEquals(512, candidateTokensTest.size());

        }
    }
}
