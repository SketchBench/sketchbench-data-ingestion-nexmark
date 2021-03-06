package edu.illinois.sketchbench.datagen;/*
   NEXMark Generator -- Niagara Extension to XMark Data Generator

   Acknowledgements:
   The NEXMark Generator was developed using the xmlgen generator 
   from the XMark Benchmark project as a basis. The NEXMark
   generator generates streams of auction elements (bids, items
   for auctions, persons) as opposed to the auction files
   generated by xmlgen.  xmlgen was developed by Florian Waas.
   See http://www.xml-benchmark.org for information.

   Copyright (c) Dept. of  Computer Science & Engineering,
   OGI School of Science & Engineering, OHSU. All Rights Reserved.

   Permission to use, copy, modify, and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies
   of the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.

   THE AUTHORS AND THE DEPT. OF COMPUTER SCIENCE & ENGINEERING 
   AT OHSU ALLOW USE OF THIS SOFTWARE IN ITS "AS IS" CONDITION, 
   AND THEY DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES 
   WHATSOEVER RESULTING FROM THE USE OF THIS SOFTWARE.

   This software was developed with support from NSF ITR award
   IIS0086002 and from DARPA through NAVY/SPAWAR 
   Contract No. N66001-99-1-8098.

*/

/**
 * @author Kristin Tufte
 *
 * To change the template for this generated type comment go to
 * Window>Preferences>Java>Code Generation>Code and Comments
 */

public class NEXMarkGenerator {

    // DEFAULT SETTINGS - modify as desired.
    // number of times the generator is called
    public static int DEFAULT_GEN_CALLS = 1000;

    // print nicely or print without newlines and indentation
    // (In our experience newlines and indentation significantly
    // slow down parsing.)  
    public static boolean DEFAULT_PRETTYPRINT = true;


    public static String DEFAULT_KAFKA_ENDPOINT = "localhost:9092";
    public static String DEFAULT_KAFKA_TOPIC = "nexmarkStream";
    public static int DEFAULT_SLEEP_MILLIS = 100;

    public static void main(String[] args) {
        try {
            // set defaults then process args
            // num times to run generator
            int gencalls = DEFAULT_GEN_CALLS;
            // print with formatting or not
            boolean prettyprint = DEFAULT_PRETTYPRINT;

            String kafkaEndpoint = DEFAULT_KAFKA_ENDPOINT;
            String topic = DEFAULT_KAFKA_TOPIC;
            int sleepMillis = DEFAULT_SLEEP_MILLIS;

            // process args
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-help")) {
                    usage();
                } else if (args[i].equals("-gen-calls")) {
                    gencalls = Integer.parseInt(args[i + 1]);
                    i++; // increment past argument   
                } else if (args[i].equals("-prettyprint")) {
                    prettyprint = Boolean.valueOf(args[i + 1]).booleanValue();
                    i++;
                } else if (args[i].equals("-kafkaBootServers")) {
                    kafkaEndpoint = args[i + 1];
                    i++;
                } else if (args[i].equals("-topic")) {
                    topic = args[i + 1];
                    i++;
                } else if (args[i].equals("-sleep")) {
                    sleepMillis = Integer.valueOf(args[i + 1]).intValue();
                    i++;
                }else {
                    usage();
                }
            }

            // create generator
            JSONAuctionStreamGenerator generator =
                new JSONAuctionStreamGenerator(gencalls, kafkaEndpoint, topic, sleepMillis);

            generator.generateStream();

        } catch (java.io.IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }

    private static void usage() {
        System.out.println(
            "Usage: NexMarkGenerator [-gen-calls gencalls]"
                + " [-prettyprint true|false]");
        System.exit(-1);
    }
}
