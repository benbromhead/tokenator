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
import java.util.stream.IntStream;

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


    static {
        Option optHelp = new Option(HELP_OPTION, "Display help");
        options.addOption(optHelp);

        Option optHost = new Option(HOST_OPTION, "host", true, "Host to connect to");
        optHost.setArgName("host");
        options.addOption(optHost);

        Option optPort = new Option(PORT_OPTION, "port", true, "Port to connect to");
        optPort.setArgName("port");
        options.addOption(optPort);


        Option optUsername = new Option(USERNAME_OPTION, "username", true, "Username");
        optUsername.setArgName("username");
        options.addOption(optUsername);

        Option optPassword = new Option(PASSWORD_OPTION, "password", true, "Password");
        optPassword.setArgName("password");
        options.addOption(optPassword);
    }




    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ic-tokenator <keyspace>", "Calculate non overlapping tokens for vnodes over a single keyspace", options, null);
        System.exit(0);
    }

    public static long getTokenRangeSize(TokenRange tr) {
        return tr.unwrap()
                .stream()
                .map(trr -> Math.sqrt(((long) trr.getEnd().getValue() - (long) trr.getStart().getValue())^2))
                .reduce(0.0, Double::sum).longValue();
    }

    public static HashSet<TokenRange> generateCandidates(final Map<Token, Set<Token>> primaryToReplicaTokens, final List<Token> ring, final Metadata metadata) {
        HashSet<TokenRange> candidateTokens = new HashSet<>();
        HashSet<Token> tokensWithRangeMovements = new HashSet<>();
        List<TokenRange> orderedTokenRanges = IntStream.range(0, ring.size()).boxed()
                .map(i -> metadata.newTokenRange(ring.get(i) , ring.get((i + 1) % ring.size())))
                .sorted(Comparator.comparing(Tokenator::getTokenRangeSize, Comparator.reverseOrder()))
                .collect(Collectors.toList());

        logger.info("Top 10 ranges");
        orderedTokenRanges.subList(0, 10).forEach(tr -> logger.info("Token: {}      Size: {}", tr.getStart(), getTokenRangeSize(tr)));

        Streams.forEachPair(orderedTokenRanges.stream(), // token range stream
                IntStream.range(0, orderedTokenRanges.size()).boxed(), //stream of ints to track index
                (tr, i) -> {
                    if(tokensWithRangeMovements.contains(tr.getStart()) || primaryToReplicaTokens.get(tr.getStart()).stream().anyMatch(tokensWithRangeMovements::contains)) {
                        logger.trace("Arghh found clash!!! " + tr.getStart());
                    } else {
                        candidateTokens.add(metadata.newTokenRange(tr.getStart() , ring.get((i + 1) % ring.size())));
                        tokensWithRangeMovements.add(tr.getStart()); //TODO this is probably redundant
                        tokensWithRangeMovements.addAll(primaryToReplicaTokens.get(tr.getStart()));
                    }
                });

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
        } else {
            logger.error("must pass arguments in");
            printHelp();
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

            logger.info("Found " + candidateTokens.size() + " non-overlapping tokens for bootstrapping nodes");
            logger.info("We can bootstrap " + candidateTokens.size() / numTokensNode + " nodes at once");
            List<com.datastax.driver.core.Token> tokens = candidateTokens.stream()
                    .map(tr -> tr.splitEvenly(1).get(0).getEnd())
                    .collect(Collectors.toList());

            for(int i = 0; i < tokens.size() / numTokensNode; i++) {
                logger.info("initial_token: " + tokens.subList(i * numTokensNode, numTokensNode + i * numTokensNode).stream().map(com.datastax.driver.core.Token::toString).collect(Collectors.joining(",")));
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

    public static Cluster connect(InetAddress address, int port, AuthProvider authProvider, boolean sslEnabled) {
        final Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPoints(address)
                .withPort(port);
        if (authProvider != null) {
            clusterBuilder.withAuthProvider(authProvider);
        }
        if (sslEnabled) {
            clusterBuilder.withSSL();
        }
        return clusterBuilder.build();
    }



}
