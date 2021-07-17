package edu.illinois.sketchbench.datagen;

import org.apache.kafka.clients.producer.Producer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class JSONAuctionStreamGenerator extends Thread{

    String tab = "";
    String tab2 = "";
    String tab3 = "";
    String nl = "";


    String empty_json = "\"\"";

    public static int ITEMS_PER_PERSON = 10;
    public static int BIDS_PER_ITEM = 10;
    public static final String yesno[] = { "yes", "no"};
    public static final String auction_type[] = {"Regular", "Featured"};
    public static final String XML_DECL = "\n<?xml version=\"1.0\"?>\n";

    // will generate bids, items and persons in a ratio of 10 bids/item 5 items/person
    private Random rnd = new Random(103984);
    private SimpleCalendar cal = new SimpleCalendar(rnd);

    private Persons persons = new Persons(); // for managing person ids
    private OpenAuctions openAuctions; // for managing open auctions
    private PersonGen p = new PersonGen();  // used for generating values for person

    private MyBuffer myBuf;
    private BufferedWriter writer;
    private int numGenCalls;
    private boolean usePrettyPrint;
    private String kafkaBootServers;
    private String kafkaTopic;
    private int threadSleep;

    private static int MAXINCREMENT_MILLISEC = 1000;
    private static int WARP = 10;
    private static int DELAY = 24000;

    public static int DEFAULT_GEN_CALLS = 3000;
    public static boolean DEFAULT_PRETTYPRINT = true;

    public boolean LIMIT_ATTRIBUTES = true;
    private static final Logger log = Logger.getLogger(JSONAuctionStreamGenerator.class);
    JSONAuctionStreamGenerator auctionStreamGenerator;

    Producer<String, String> producer1;
    static final ReentrantLock lock = new ReentrantLock();

    long startTime2;
    private int temp = 0;

    public JSONAuctionStreamGenerator(int genCalls, boolean prettyprint){

        numGenCalls = genCalls;
        usePrettyPrint = prettyprint;
        openAuctions = new OpenAuctions(cal);

        if(usePrettyPrint) {
            tab = "\t";
            tab2 = "\t\t";
            tab3 = "\t\t\t";
            nl = "\n";
        }
    }

    public JSONAuctionStreamGenerator(int genCalls, String kafkaEndpoint, String topic, int sleepMillis){

        numGenCalls = genCalls;
        kafkaBootServers = kafkaEndpoint;
        kafkaTopic = topic;
        threadSleep = sleepMillis;
        openAuctions = new OpenAuctions(cal);
        producer1 = KafkaMessageSender.createProducer(kafkaBootServers,kafkaTopic);
        if(usePrettyPrint) {
            tab = "\t";
            tab2 = "\t\t";
            tab3 = "\t\t\t";
            nl = "\n";
        }
    }

    public JSONAuctionStreamGenerator(int genCalls, boolean prettyprint, JSONAuctionStreamGenerator auctionStreamGenerator){

        numGenCalls = genCalls;
        usePrettyPrint = prettyprint;
        //openAuctions = new OpenAuctions(cal);
        openAuctions = auctionStreamGenerator.openAuctions;
        if(usePrettyPrint) {
            tab = "\t";
            tab2 = "\t\t";
            tab3 = "\t\t\t";
            nl = "\n";
        }
        this.auctionStreamGenerator = auctionStreamGenerator;
    }
    public void generateStream() throws IOException {

        if(LIMIT_ATTRIBUTES)
            System.out.println("WARNING: LIMITING ATTRIBUTES");

        // first do startup - generate some people and open auctions that
        // can be bid on

        // put 10 persons in a document
        for(int i = 0;i<5; i++) {
            for(int j = 0; j<10; j++) {
                generatePerson(1);
            }
        }

        for(int i = 0; i<5; i++) {
            for(int j = 0; j<10; j++) {
                generateOpenAuction(1);
            }
        }

        // now go into a loop generating bids and persons and so on
        // want on average 10 items/person and 10 bids/item
        int count = 0;
      //  while(count < numGenCalls) {
        while(true) {

            // generating a person approximately 10th time will
            // give is 10 items/person since we generate on average
            // one bid per loop
            if(rnd.nextInt(10) == 0) { generatePerson(1);
            }

            // want on average 1 item and 10 bids
            int numItems = rnd.nextInt(3); // should average 1
            generateOpenAuction(numItems);

            int numBids = rnd.nextInt(21); // should average 10
            generateBid(numBids);


            count++;
        }

    }

    public void incrementCommon() {
        auctionStreamGenerator.temp++;

        if (auctionStreamGenerator.temp == 1) {
            auctionStreamGenerator.startTime2 = System.currentTimeMillis();
        }
        long diff = System.currentTimeMillis() - auctionStreamGenerator.startTime2;
        log.info(Thread.currentThread().getName() + " spent : "
                + diff + " for the event count : " + auctionStreamGenerator.temp
                + " with the  Data rate : " + (auctionStreamGenerator.temp * 1000  / diff));
    }

    private void initMyBuf() throws IOException {
        myBuf.clear();
        myBuf.append(XML_DECL);
        myBuf.append("<site>");
        myBuf.append(nl);
    }

    private void writeMyBuf() throws IOException {
        myBuf.append("</site>");
        myBuf.append(nl);
        writer.write(myBuf.array(), 0, myBuf.length());
    }

    private void generateBid(int numBids) throws IOException {
        long ts=0, temp;
        boolean start=true;

        cal.incrementTime();
        // myb.append("<open_auctions>");

        // myb.append(nl);

        for (int i=0; i<numBids;i++) {
            StringBuilder jsonDataItem = new StringBuilder();

            int itemId = openAuctions.getExistingId();

            jsonDataItem.append("{ \"event\": { ");
            jsonDataItem.append("\"iij_timestamp\"");
            jsonDataItem.append(":");
            jsonDataItem.append(System.currentTimeMillis());
            jsonDataItem.append(",");


            jsonDataItem.append("\"auction_id\"");
            jsonDataItem.append(":");
            jsonDataItem.append(itemId);
            jsonDataItem.append(",");

            jsonDataItem.append("\"partition_id\"");
            jsonDataItem.append(":");
            jsonDataItem.append(itemId%4);
            jsonDataItem.append(",");




            //here, datetime is in second;
            // Alternate time stamps
            //myb.append(System.currentTimeMillis() - rnd.nextInt(MAXINCREMENT_MILLISEC));
            //ts = System.currentTimeMillis() * WARP + DELAY;
            //myb.append(ts);



            jsonDataItem.append("\"time\"");
            jsonDataItem.append(":");
            // jsonDataItem.append(cal.getTimeInSecs());
            jsonDataItem.append(System.currentTimeMillis() - rnd.nextInt(MAXINCREMENT_MILLISEC));
            jsonDataItem.append(",");


            jsonDataItem.append("\"person\"");
            jsonDataItem.append(":");
            jsonDataItem.append(persons.getExistingId());
            jsonDataItem.append(",");

            jsonDataItem.append("\"bid\"");
            jsonDataItem.append(":");
            jsonDataItem.append(openAuctions.increasePrice(itemId));

            jsonDataItem.append(" } }");
            try {

//                if((int)Thread.currentThread().getId()%4 == 0) {
//                    KafkaMessageSender.runProducer1(jsonDataItem.toString(),producer1);
//                } else if ((int)Thread.currentThread().getId()%4 == 1) {
//                    KafkaMessageSender.runProducer2(jsonDataItem.toString(),producer1);
//                } else if ((int)Thread.currentThread().getId()%4 == 2) {
//                    KafkaMessageSender.runProducer3(jsonDataItem.toString(),producer1);
//                } else {
//                    KafkaMessageSender.runProducer4(jsonDataItem.toString(),producer1);
//                }
                KafkaMessageSender.runProducer1(jsonDataItem.toString(),producer1);

//                log.info("Message from Stream3 sent to kafka by "
//                        + Thread.currentThread().getName());
//
//                incrementCommon();



                try {
                    Thread.currentThread().sleep(threadSleep);
                } catch (InterruptedException e) {
                    log.info("Error: " + e.getMessage());
                }

            } catch (InterruptedException e) {
                log.error("Error sending an event to Input Handler, " + e.getMessage(), e);
            } catch (Exception e) {
                log.error("Error: " + e.getMessage(), e);
            }

        }

    }

    // uugh, a bad thing here is that a person can be selling items that are in
    // different regions, ugly, but to keep it consistent requires maintaining
    // too much data and also I don't think this will affect results
    private void generateOpenAuction(int numItems) throws IOException {
        cal.incrementTime();



        // open auction contains:
        // initial, reserve?, bidder*, current, privacy?, itemref, seller, annotation,
        // quantity, type, interval

        for (int i=0; i<numItems; i++) {
            // at this point we are not generating items, we are generating
            // only open auctions, id for open_auction is same as id of item
            // up for auction


            StringBuilder jsonDataItem = new StringBuilder();


            lock.lock();
            int auctionId = openAuctions.getNewId();
            lock.unlock();


            jsonDataItem.append("{ \"event\": { ");
            jsonDataItem.append("\"iij_timestamp\"");
            jsonDataItem.append(":");
            jsonDataItem.append(System.currentTimeMillis());
            jsonDataItem.append(",");


            jsonDataItem.append("\"auction_id\"");
            jsonDataItem.append(":");
            jsonDataItem.append(auctionId);
            jsonDataItem.append(",");
//
//            jsonDataItem.append("\"partition_id\"");
//            jsonDataItem.append(":");
//            jsonDataItem.append(auctionId%noOfPartialSosddhiApps);
//            jsonDataItem.append(",");


            // no initial - does not fit our scenario

            if(!LIMIT_ATTRIBUTES) {
                // reserve
                if(rnd.nextBoolean()) {

                    jsonDataItem.append("\"reserve\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append((int)Math.round((openAuctions.getCurrPrice(auctionId))*(1.2+(rnd.nextDouble()+1))));
                    jsonDataItem.append(",");
                } else {
                    jsonDataItem.append("\"reserve\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(-1);
                    jsonDataItem.append(",");
                }
                // no bidders

                // no current - do with accumlator

                // privacy
                if(rnd.nextBoolean()) {

                    jsonDataItem.append("\"privacy\"");
                    jsonDataItem.append(":\"");
                    jsonDataItem.append(yesno[rnd.nextInt(2)]);
                    jsonDataItem.append("\",");
                } else {
                    jsonDataItem.append("\"privacy\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(-1);
                    jsonDataItem.append(",");
                }
            }

            // itemref


            // assume itemId and openAuctionId are same - only one auction per item allowed




            jsonDataItem.append("\"item_id\"");
            jsonDataItem.append(":");
            jsonDataItem.append(auctionId);
            jsonDataItem.append(",");

            // seller


            jsonDataItem.append("\"seller\"");
            jsonDataItem.append(":");
            jsonDataItem.append(persons.getExistingId());
            jsonDataItem.append(",");

            // skip annotation - too hard to generate - need to just get this done KT

            // KT - add category id XMark items can be in 1-10 categories
            // we allow an item to be in one category


            int catid = rnd.nextInt(303);


            jsonDataItem.append("\"category\"");
            jsonDataItem.append(":");
            jsonDataItem.append(catid);
            jsonDataItem.append(",");

            if(!LIMIT_ATTRIBUTES) {
                // quantity

                int quantity = 1+rnd.nextInt(10);
                jsonDataItem.append("\"quantity\"");
                jsonDataItem.append(":");
                jsonDataItem.append(quantity);
                jsonDataItem.append(",");


                jsonDataItem.append("\"auction_type\"");
                jsonDataItem.append(":\"");
                jsonDataItem.append(auction_type[rnd.nextInt(2)]);
                if(quantity>1 && rnd.nextBoolean())
                    jsonDataItem.append(", Dutch"); //
                jsonDataItem.append("\",");


                // interval

                // myb.append(tab2);
                // myb.append("<interval>");
                // myb.append("<start>");
                // myb.append(cal.getTimeInSecs());
                // myb.append("</start>");
                // myb.append("<end>");
                // myb.append(openAuctions.getEndTime(auctionId));

                //myb.append(System.currentTimeMillis() * WARP + DELAY + rnd.nextInt(MAXINCREMENT_MILLISEC));

                // myb.append("</end>");
                // myb.append("</interval>");
                // myb.append(nl);

                jsonDataItem.append("\"starting\"");
                jsonDataItem.append(":");
                //jsonDataItem.append(cal.getTimeInSecs());
                jsonDataItem.append(System.currentTimeMillis());
                jsonDataItem.append(",");


                jsonDataItem.append("\"ending\"");
                jsonDataItem.append(":");
                //jsonDataItem.append(openAuctions.getEndTime(auctionId));
                jsonDataItem.append(System.currentTimeMillis() * WARP + DELAY + rnd.nextInt(MAXINCREMENT_MILLISEC));




            }

            jsonDataItem.append(" } }");
            try {


//                KafkaMessageSender.runProducer2(jsonDataItem.toString(),producer1);
//                log.info("Message from Stream2 sent to kafaka by "
//                        + Thread.currentThread().getName());
//
//                              incrementCommon();



                try {
                    Thread.currentThread().sleep(threadSleep);
                } catch (InterruptedException e) {
                    log.info("Error: " + e.getMessage());
                }

//            } catch (InterruptedException e) {
//                log.error("Error sending an event to Input Handler, " + e.getMessage(), e);
            } catch (Exception e) {
                log.error("Error: " + e.getMessage(), e);
            }
        }

    }

    // append region AFRICA, ASIA, AUSTRALIA, EUROPE, NAMERICA, SAMERICA
    //Item contains:
    // location, quantity, name, payment, description, shipping, incategory+, mailbox)>
    // weird, item doesn't contain a reference to the seller, open_auction contains
    // a reference to the item and a reference to the seller


    private void generatePerson(int numPersons) throws IOException {
        cal.incrementTime();

        for (int i=0; i<numPersons; i++) {
            StringBuilder jsonDataItem = new StringBuilder();
            p.generateValues(openAuctions); // person object is reusable now

            jsonDataItem.append("{ \"event\": { ");
            jsonDataItem.append("\"iij_timestamp\"");
            jsonDataItem.append(":");
            jsonDataItem.append(System.currentTimeMillis());
            jsonDataItem.append(",");

            jsonDataItem.append("\"person_id\"");
            jsonDataItem.append(":");
            jsonDataItem.append(persons.getNewId());
            jsonDataItem.append(",");

            jsonDataItem.append("\"name\"");
            jsonDataItem.append(":\"");
            jsonDataItem.append(p.m_stName.toString().trim());
            jsonDataItem.append("\",");

            jsonDataItem.append("\"email\"");
            jsonDataItem.append(":\"");
            jsonDataItem.append(p.m_stEmail.toString().trim());
            jsonDataItem.append("\",");

            if(!LIMIT_ATTRIBUTES) {
                if (p.has_phone) {

                    jsonDataItem.append("\"phone\"");
                    jsonDataItem.append(":\"");
                    jsonDataItem.append(p.m_stPhone.toString().trim());
                    jsonDataItem.append("\",");

                } else{

                    jsonDataItem.append("\"phone\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(empty_json);
                    jsonDataItem.append(",");

                }
                if (p.has_address) {

                    jsonDataItem.append("\"street\"");
                    jsonDataItem.append(":\"");
                    jsonDataItem.append(p.m_address.m_stStreet.toString().trim());
                    jsonDataItem.append("\",");

                    jsonDataItem.append("\"city\"");
                    jsonDataItem.append(":\"");
                    jsonDataItem.append(p.m_address.m_stCity);
                    jsonDataItem.append("\",");

                    jsonDataItem.append("\"country\"");
                    jsonDataItem.append(":\"");
                    jsonDataItem.append(p.m_address.m_stCountry);
                    jsonDataItem.append("\",");


                    jsonDataItem.append("\"province\"");
                    jsonDataItem.append(":\"");
                    jsonDataItem.append(p.m_address.m_stProvince);
                    jsonDataItem.append("\",");


                    jsonDataItem.append("\"zipcode\"");
                    jsonDataItem.append(":\"");
                    jsonDataItem.append(p.m_address.m_stZipcode);
                    jsonDataItem.append("\",");

                } else {

                    jsonDataItem.append("\"street\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(empty_json);
                    jsonDataItem.append(",");

                    jsonDataItem.append("\"city\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(empty_json);
                    jsonDataItem.append(",");

                    jsonDataItem.append("\"country\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(empty_json);
                    jsonDataItem.append(",");


                    jsonDataItem.append("\"province\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(empty_json);
                    jsonDataItem.append(",");

                    jsonDataItem.append("\"zipcode\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(empty_json);
                    jsonDataItem.append(",");

                }
                if (p.has_homepage) {

                    jsonDataItem.append("\"homepage\"");
                    jsonDataItem.append(":\"");
                    jsonDataItem.append(p.m_stHomepage.toString().trim());
                    jsonDataItem.append("\",");
                } else {

                    jsonDataItem.append("\"homepage\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(empty_json);
                    jsonDataItem.append(",");
                }
                if (p.has_creditcard) {


                    jsonDataItem.append("\"creditcard\"");
                    jsonDataItem.append(":\"");
                    jsonDataItem.append(p.m_stCreditcard.toString().trim());
                    jsonDataItem.append("\",");
                } else {

                    jsonDataItem.append("\"creditcard\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(empty_json);
                    jsonDataItem.append(",");
                }

                if (p.has_profile) {

                    jsonDataItem.append("\"profile_income\"");
                    jsonDataItem.append(":\"");
                    jsonDataItem.append(p.m_profile.m_stIncome.toString().trim());
                    jsonDataItem.append("\",");

                    if (p.m_profile.has_education) {

                        jsonDataItem.append("\"education\"");
                        jsonDataItem.append(":\"");
                        jsonDataItem.append(p.m_profile.m_stEducation);
                        jsonDataItem.append("\",");
                    } else {

                        jsonDataItem.append("\"education\"");
                        jsonDataItem.append(":");
                        jsonDataItem.append(empty_json);
                        jsonDataItem.append(",");
                    }
                    if (p.m_profile.has_gender) {

                        jsonDataItem.append("\"gender\"");
                        jsonDataItem.append(":\"");
                        jsonDataItem.append(p.m_profile.m_stGender);
                        jsonDataItem.append("\",");
                    } else {

                        jsonDataItem.append("\"gender\"");
                        jsonDataItem.append(":");
                        jsonDataItem.append(empty_json);
                        jsonDataItem.append(",");
                    }

                    jsonDataItem.append("\"business\"");
                    jsonDataItem.append(":\"");
                    jsonDataItem.append(p.m_profile.m_stBusiness);
                    jsonDataItem.append("\",");

                    if (p.m_profile.has_age) {

                        jsonDataItem.append("\"age\"");
                        jsonDataItem.append(":\"");
                        jsonDataItem.append(p.m_profile.m_stAge);
                        jsonDataItem.append("\"");
                    } else {

                        jsonDataItem.append("\"age\"");
                        jsonDataItem.append(":");
                        jsonDataItem.append(empty_json);

                    }

                } else {

                    jsonDataItem.append("\"profile_income\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(empty_json);
                    jsonDataItem.append(",");

                    jsonDataItem.append("\"education\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(empty_json);
                    jsonDataItem.append(",");

                    jsonDataItem.append("\"gender\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(empty_json);
                    jsonDataItem.append(",");

                    jsonDataItem.append("\"business\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(empty_json);
                    jsonDataItem.append(",");

                    jsonDataItem.append("\"age\"");
                    jsonDataItem.append(":");
                    jsonDataItem.append(empty_json);

                }

            }


            jsonDataItem.append(" } }");
            try {

//                KafkaMessageSender.runProducer1(jsonDataItem.toString(),producer1);
//                log.info("Message from Stream1 sent to kafaka by "
//                        + Thread.currentThread().getName());
//
//               incrementCommon();


                try {
                    Thread.currentThread().sleep(threadSleep);
                } catch (InterruptedException e) {
                    log.info("Error: " + e.getMessage());
                }

//            } catch (InterruptedException e) {
//                log.error("Error sending an event to Input Handler, " + e.getMessage(), e);
            } catch (Exception e) {
                log.error("Error: " + e.getMessage(), e);
            }
        }

    }

    public static void main (String args[]){

        BasicConfigurator.configure();
        log.info("Welcome to kafka message sender");

        JSONAuctionStreamGenerator auctionStreamGenerator1 = new JSONAuctionStreamGenerator(DEFAULT_GEN_CALLS,DEFAULT_PRETTYPRINT);
        JSONAuctionStreamGenerator auctionStreamGenerator2 = new JSONAuctionStreamGenerator(DEFAULT_GEN_CALLS,DEFAULT_PRETTYPRINT, auctionStreamGenerator1);
        JSONAuctionStreamGenerator auctionStreamGenerator3 = new JSONAuctionStreamGenerator(DEFAULT_GEN_CALLS,DEFAULT_PRETTYPRINT, auctionStreamGenerator1);
        JSONAuctionStreamGenerator auctionStreamGenerator4 = new JSONAuctionStreamGenerator(DEFAULT_GEN_CALLS,DEFAULT_PRETTYPRINT, auctionStreamGenerator1);
        JSONAuctionStreamGenerator auctionStreamGenerator5 = new JSONAuctionStreamGenerator(DEFAULT_GEN_CALLS,DEFAULT_PRETTYPRINT, auctionStreamGenerator1);
        JSONAuctionStreamGenerator auctionStreamGenerator6 = new JSONAuctionStreamGenerator(DEFAULT_GEN_CALLS,DEFAULT_PRETTYPRINT, auctionStreamGenerator1);
        JSONAuctionStreamGenerator auctionStreamGenerator7 = new JSONAuctionStreamGenerator(DEFAULT_GEN_CALLS,DEFAULT_PRETTYPRINT, auctionStreamGenerator1);
        JSONAuctionStreamGenerator auctionStreamGenerator8 = new JSONAuctionStreamGenerator(DEFAULT_GEN_CALLS,DEFAULT_PRETTYPRINT, auctionStreamGenerator1);
        JSONAuctionStreamGenerator auctionStreamGenerator9 = new JSONAuctionStreamGenerator(DEFAULT_GEN_CALLS,DEFAULT_PRETTYPRINT, auctionStreamGenerator1);
        JSONAuctionStreamGenerator auctionStreamGenerator10 = new JSONAuctionStreamGenerator(DEFAULT_GEN_CALLS,DEFAULT_PRETTYPRINT, auctionStreamGenerator1);
        auctionStreamGenerator1.start();
        auctionStreamGenerator2.start();
        auctionStreamGenerator3.start();
        auctionStreamGenerator4.start();
        auctionStreamGenerator5.start();
        auctionStreamGenerator6.start();
        auctionStreamGenerator7.start();
        auctionStreamGenerator8.start();
        auctionStreamGenerator9.start();
        auctionStreamGenerator10.start();

    }

    public void run() {
        try {
            generateStream();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
