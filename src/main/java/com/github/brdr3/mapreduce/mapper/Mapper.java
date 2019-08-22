package com.github.brdr3.mapreduce.mapper;

import com.github.brdr3.mapreduce.util.Message;
import com.github.brdr3.mapreduce.util.Message.MessageBuilder;
import com.github.brdr3.mapreduce.util.User;
import com.github.brdr3.mapreduce.util.constants.Constants;
import com.google.gson.Gson;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import java.util.List;

public class Mapper {
    private User mapperUser;
    private final Thread receiver;
    private final Thread sender;
    private final Thread processor;
    private final ConcurrentLinkedQueue<Message> processQueue;
    private final ConcurrentLinkedQueue<Message> senderQueue;

    static Logger logger = Logger.getLogger("log4j.properties");

    public Mapper(int port) {
        mapperUser = new User(-2, Constants.getRealInetAddress(), port);
        
        receiver = new Thread() {
            @Override
            public void run() {
                receive();
            }
        };
        
        sender = new Thread() {
            @Override
            public void run() {
                send();
            }
        };
        
        processor = new Thread() {
            @Override
            public void run() {
                process();
            }
        };
        
        processQueue = new ConcurrentLinkedQueue<>();
        senderQueue = new ConcurrentLinkedQueue<>();
        
        Message m = new MessageBuilder()
                               .to(Constants.coordinatorServer)
                               .from(mapperUser)
                               .content(mapperUser)
                               .build();
        senderQueue.add(m);
        logger.info("Mapper" + mapperUser + " -> avisando coordenador.");
    }

    public void start() {
        receiver.start();
        sender.start();
        processor.start();
    }
    
    public void receive() {
        DatagramSocket socket;
        DatagramPacket packet;
        String jsonMessage;
        Message message;

        byte buffer[] = new byte[65507];

        Gson gson = new Gson();

        try {
            socket = new DatagramSocket(mapperUser.getPort());
            while (true) {
                sleep();
                packet = new DatagramPacket(buffer, buffer.length, 
                                            mapperUser.getAddress(), 
                                            mapperUser.getPort());

                socket.receive(packet);
                jsonMessage = new String(packet.getData()).trim();
                message = gson.fromJson(jsonMessage, Message.class);
                processQueue.add(message);
                logger.info("Mapper" + mapperUser + " -> mensagem chegou e foi add na fila processQueue");
                cleanBuffer(buffer);

            }
        } catch (Exception ex) {
            logger.warning("Mapper" + mapperUser +" -> " + ex );
        }
    }
    
    public void send() {
        while (true) {
            sleep();
            Message urls = senderQueue.poll();
            if(urls != null) {
                logger.info("Mapper" + mapperUser +" -> enviando mensagem");
                sendMessage(urls);
            }
        }
    }
    
    public void sendMessage(Message m) {
        Gson gson = new Gson();
        String jsonMessage = gson.toJson(m);
        DatagramSocket socket;
        DatagramPacket packet;
        byte buffer[] = jsonMessage.getBytes();
        packet = new DatagramPacket(buffer, buffer.length, m.getTo().getAddress(),
                                    m.getTo().getPort());
        try {
            socket = new DatagramSocket();
            socket.send(packet);
            logger.info("Mapper" + mapperUser +" -> mensagem enviada com sucesso");
            socket.close();
        } catch (Exception ex) {
            logger.warning("Mapper" + mapperUser +" -> mensagem não enviada " + ex );
        }
    }
    
    public User getMapperUser() {
        return mapperUser;
    }

    public void setMapperUser(User mapperUser) {
        this.mapperUser = mapperUser;
    }
    
    public void process() {
        while(true) {
            sleep();
            Message m = processQueue.poll();
            if(m != null) {
                logger.info("Mapper" + mapperUser +" -> mensagem enviada para processamento");
                processMessage(m);
            }
        }
    }
    
    public void processMessage(Message m) {
        List<String> listToProcess;
        if (m.getContent() instanceof List)
            listToProcess = (List<String>) m.getContent();
        else {
            logger.warning("Mapper" + mapperUser + " -> mensagem fora do padrao");
            return;
        }

        HashMap<String, List<String>> linksUrls = new HashMap<>();
        Document website;
        Elements links;

        logger.info("Mapper" + mapperUser +" -> Processando lista com " + listToProcess.size() + " urls");
        for(String url: listToProcess) {
            try {
                website = Jsoup.connect(url).get();
                logger.info("Mapper" + mapperUser +" -> acessou o site: <" + url + ">");
                links = website.select("a[href]");
                
                List<String> linkList =
                    links.stream()
                         .map(l -> l.attr("href"))
                         .collect(Collectors.toCollection(LinkedList :: new));
                
                linksUrls.put(url, linkList);
                
            } catch (Exception ex) {
                logger.warning("Mapper" + mapperUser +" -> não foi possivel acessar o site: <" + url + ">");
                linksUrls.put(url, new ArrayList<String>());
            }
        }
        
        Message messageToReducer = new MessageBuilder()
                                        .id(m.getId())
                                        .end(m.getEnd())
                                        .to(Constants.reducerServer)
                                        .from(mapperUser)
                                        .content(linksUrls)
                                        .requestor(m.getRequestor())
                                        .build();
        logger.info("Mapper" + mapperUser + " -> mensagem foi adicionada na senderQueue");
        senderQueue.add(messageToReducer);
    }
    
    private void cleanBuffer(byte[] buffer) {
        for(int i = 0; i < buffer.length; i++) {
            buffer[i] = 0;
        }
    }

    private  void sleep(){
        try {
            Thread.sleep(1);
        } catch ( InterruptedException e ){
            System.out.println(e);
        }
    }
}
