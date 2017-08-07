package com.instaclustr.tokenator;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.common.collect.*;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Collectors;

public class Tokenator {
    private static final Logger logger = LoggerFactory.getLogger(Tokenator.class);

    private static final int DEFAULT_PORT = 9042;
    private static final String USERNAME_OPTION = "u";
    private static final String HOST_OPTION = "h";
    private static final String PASSWORD_OPTION = "pw";
    private static final String HELP_OPTION = "help";
    private static final String DESIRED_TOKENS_OPTION = "t";
    private static final String PORT_OPTION = "p";
    private static final InetAddress IPV4_LOOPBACK = Inet4Address.getLoopbackAddress();
    private static int DEFAULT_TOKENS = 256;

    final Multimap<InetAddress, Token> unitToTokens = HashMultimap.create();


    private static final Options options = new Options();


    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ic-tokenator <keyspace>", "Calculate non overlapping tokens for vnodes over a single keyspace", options, null);

    }

    public static HashSet<TokenRange> generateCandidates(final Map<Token, Set<Token>> primaryToReplicaTokens, final List<Token> ring, final Metadata metadata) {
        HashSet<TokenRange> candidateTokens = new HashSet<>();
        HashSet<Token> tokensWithRangeMovements = new HashSet<>();

        int index = 0;
        for (com.datastax.driver.core.Token key : ring) {
            if(tokensWithRangeMovements.contains(key) || primaryToReplicaTokens.get(key).stream().anyMatch(tokensWithRangeMovements::contains)) {
                logger.trace("Arghh found clash!!! " + key);
                index++;
                continue;
            }
            candidateTokens.add(metadata.newTokenRange(key , ring.get((index + 1) % ring.size())));
            tokensWithRangeMovements.add(key); //TODO this is probably redundant
            tokensWithRangeMovements.addAll(primaryToReplicaTokens.get(key));
            index++;
        }
        return candidateTokens;
    }


    public static void main(String[] args) {

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            logger.error("Command line parse error", e);
            System.exit(1);
        }

        if (cmd.hasOption(HELP_OPTION)) {
            printHelp();
            System.exit(0);
        }

        int numTokensNode = DEFAULT_TOKENS;
        if(cmd.hasOption(DESIRED_TOKENS_OPTION)) {
            try {
                numTokensNode = Integer.parseInt(cmd.getOptionValue(DESIRED_TOKENS_OPTION));
            } catch (NumberFormatException e) {
                logger.error("Invalid number of tokens", e);
                System.exit(1);
            }
        }

        int port = DEFAULT_PORT;
        if (cmd.hasOption(PORT_OPTION)) {
            try {
                port = Integer.parseInt(cmd.getOptionValue(PORT_OPTION));
            } catch (NumberFormatException e) {
                logger.error("Invalid port", e);
                System.exit(1);
            }
        }

        if (cmd.hasOption(USERNAME_OPTION) && !cmd.hasOption(PASSWORD_OPTION)) {
            logger.error("Must provide password with username!");
            System.exit(1);
        }
        if (cmd.hasOption(PASSWORD_OPTION) && !cmd.hasOption(USERNAME_OPTION)) {
            logger.error("Must provide username with password!");
            System.exit(1);
        }
        InetAddress address = IPV4_LOOPBACK;
        if (cmd.hasOption(HOST_OPTION)) {
            try {
                address = Inet4Address.getByName(cmd.getOptionValue(HOST_OPTION));
            } catch (UnknownHostException e) {
                logger.error("Unknown host", e);
                System.exit(1);
            }
        }


        List<String> arguments = cmd.getArgList();
        String keyspaceName = null;
        if (!arguments.isEmpty()) {
            keyspaceName = arguments.get(0);
        }

        AuthProvider authProvider = null;
        if (cmd.hasOption(PASSWORD_OPTION) && cmd.hasOption(USERNAME_OPTION)) {
            String username = cmd.getOptionValue(USERNAME_OPTION);
            String password = cmd.getOptionValue(PASSWORD_OPTION);
            authProvider = new PlainTextAuthProvider(username, password);
        }

        boolean sslEnabled = false; //TODO: enable SSL support

        try (final Cluster cluster = connect(address, port, authProvider, sslEnabled)) {
            final Metadata metadata = cluster.getMetadata();

            Field tokenMapField = metadata.getClass().getDeclaredField("tokenMap");
            tokenMapField.setAccessible(true);

            Field tokenToPrimaryField = tokenMapField.get(metadata).getClass().getDeclaredField("tokenToPrimary");
            tokenToPrimaryField.setAccessible(true);
            Map<com.datastax.driver.core.Token, Host> tokenToPrimary = (Map<com.datastax.driver.core.Token, Host>) tokenToPrimaryField.get(tokenMapField.get(metadata));

            Field ringField = tokenMapField.get(metadata).getClass().getDeclaredField("ring");
            ringField.setAccessible(true);
            List<com.datastax.driver.core.Token> ring = (List<com.datastax.driver.core.Token>) ringField.get(tokenMapField.get(metadata));

            ReplicationStrategyPublic replicationStrategyPublic = ReplicationStrategyPublic.create(metadata.getKeyspace(keyspaceName).getReplication());


            ReplicationStrategyPublic.TokenTopology topology = replicationStrategyPublic.computeTokenToReplicaMap(keyspaceName, tokenToPrimary, ring);

            HashSet<com.datastax.driver.core.TokenRange> candidateTokens = generateCandidates(topology.primaryToReplicaTokens, ring, metadata);

            System.out.println("Found " + candidateTokens.size() + " non-overlapping tokens for bootstrapping nodes");
            System.out.println("We can bootstrap " + candidateTokens.size() / numTokensNode + " nodes at once");
            List<com.datastax.driver.core.Token> tokens = candidateTokens.stream()
                    .map(tr -> tr.splitEvenly(1).get(0).getEnd())
                    .collect(Collectors.toList());

            for(int i = 0; i < tokens.size() / numTokensNode; i++) {
                System.out.println("initial_token: " + tokens.subList(i * numTokensNode, numTokensNode + i * numTokensNode).stream().map(com.datastax.driver.core.Token::toString).collect(Collectors.joining(",")));
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

    public static Cluster connect(InetAddress address, int port, AuthProvider authProvider, boolean sslEnabled) {
        final Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPoints(address)
                .withPort(port)
                .withQueryOptions(new QueryOptions()
                        .setConsistencyLevel(ConsistencyLevel.ALL)
                )
                .withRetryPolicy(new RetryPolicy() {
                    @Override
                    public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataRetrieved, int nbRetry) {
                        return RetryDecision.rethrow();
                    }

                    @Override
                    public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int requiredAcks, int receivedAcks, int nbRetry) {
                        return RetryDecision.rethrow();
                    }

                    @Override
                    public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry) {
                        return RetryDecision.rethrow();
                    }

                    @Override
                    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
                        return RetryDecision.rethrow();
                    }

                    @Override
                    public void init(Cluster cluster) {

                    }

                    @Override
                    public void close() {

                    }
                })
                .withSpeculativeExecutionPolicy(NoSpeculativeExecutionPolicy.INSTANCE)
                .withLoadBalancingPolicy(new WhiteListPolicy(DCAwareRoundRobinPolicy.builder().build(), ImmutableList.of(new InetSocketAddress(address, port))));
        if (authProvider != null) {
            clusterBuilder.withAuthProvider(authProvider);
        }
        if (sslEnabled) {
            clusterBuilder.withSSL();
        }
        return clusterBuilder.build();
    }



}
