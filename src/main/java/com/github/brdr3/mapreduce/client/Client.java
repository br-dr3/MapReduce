package com.github.brdr3.mapreduce.client;

import com.github.brdr3.mapreduce.util.Message;
import com.github.brdr3.mapreduce.util.Message.MessageBuilder;
import com.github.brdr3.mapreduce.util.User;
import com.github.brdr3.mapreduce.util.constants.Constants;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.internal.LinkedTreeMap;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

public class Client {

    private final User thisUser;
    private final Thread sender;
//    private final Thread receiver;
    private final Thread interactor;
    private final Thread processor;

    private File clientFolder = null;

    private ConcurrentLinkedQueue<ArrayList<String>> senderQueue;
    private ConcurrentLinkedQueue<Message> processQueue;

    static Logger logger = Logger.getLogger("log4j.properties");

    public Client() {
        int port = 14000;
        try {
            String address = Constants.getRealInetAddress();
            thisUser = new User(0, address, port);
            logger.info("Cliente@" + address + ":" + port + " instanciada com sucesso");
        } catch (Exception ex) {
            logger.info("Cliente@" + port +" não foi instanciado");
            throw new RuntimeException("It was not possible to create the User.");
        }

        sender = new Thread() {
            @Override
            public void run() {
                send();
            }
        };

//        receiver = new Thread() {
//            @Override
//            public void run() {
//                receive();
//            }
//        };

        interactor = new Thread() {
            @Override
            public void run() {
                interact();
            }
        };

        processor = new Thread() {
            @Override
            public void run() {
                process();
            }
        };

        senderQueue = new ConcurrentLinkedQueue<>();
        processQueue = new ConcurrentLinkedQueue<>();
    }

    public void start() {
        sender.start();
//        receiver.start();
        interactor.start();
        processor.start();
    }

    public void send() {
        while (true) {
            ArrayList urls = senderQueue.poll();
            sleep();
            if (urls != null) {
                sendMessage(urls);
            }
        }
    }

    public void sendMessage(ArrayList<String> urls) {

        Gson gson = new Gson();
        Message m = new MessageBuilder().from(thisUser)
                .to(Constants.coordinatorServer)
                .content(urls)
                .requestor(thisUser)
                .build();

        String jsonMessage = gson.toJson(m);
        byte buffer[] = new byte[65507];
        DatagramSocket socket;
        DatagramPacket packet;
        buffer = jsonMessage.getBytes();
        packet = new DatagramPacket(buffer, buffer.length, m.getTo().getAddress(),
                m.getTo().getPort());
        logger.info("Cliente" + thisUser + " enviando mensagem para CoordinatorServer" + Constants.coordinatorServer + " com " + urls.size() + " urls." );
        try {
            socket = new DatagramSocket();
            socket.send(packet);
            socket.close();
            logger.info("Cliente" + thisUser + " mensagem enviada para CoordinatorServer" + Constants.coordinatorServer + " com " + urls.size() + " urls.");
        } catch (Exception ex) {
            logger.info("Cliente" + thisUser + " não conseguiu enviar mensagem" + Constants.coordinatorServer + " com " + urls.size() + " urls. " + ex);

        }
    }

    public void receive() {
        DatagramSocket socket;
        DatagramPacket packet;
        String jsonMessage;
        Message message;
        byte buffer[] = new byte[65507];
        Gson gson = new Gson();

        try {
            socket = new DatagramSocket(thisUser.getPort());
//            while (true) {
                sleep();
                packet = new DatagramPacket(buffer, buffer.length, thisUser.getAddress(), thisUser.getPort());
                socket.receive(packet);
                jsonMessage = new String(packet.getData()).trim();
                message = gson.fromJson(jsonMessage, Message.class);

                processQueue.add(message);
                logger.info("Cliente" + thisUser +  " mensagem chegou e foi add na fila processQueue");
                cleanBuffer(buffer);
//            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void interact() {
        String userEntry;
        File file;
        Scanner x = null;

        if(clientFolder == null) {
            clientFolder = new File(System.getProperty("user.home") + "/Desktop/Client");
            if(!clientFolder.exists()) {
                logger.info("Cliente" + thisUser +  " criando diretório para acesso de arquivos.");
                if(clientFolder.mkdir()) {
                    logger.info("Cliente" + thisUser +  " diretório criado com sucesso.");
                } else {
                    logger.warning("Cliente" + thisUser +  " não conseguiu criar o diretório.");
                    throw new RuntimeException("Failed to create directory.");
                }
            }
        }

        while (true) {
            file = new File(clientFolder.getPath() + "/urls.txt");
            if(!file.exists()) {
                sleep();
                continue;
            }

            try {
                x = new Scanner(file);
            } catch (Exception e) {
                logger.warning("Cliente" + thisUser +  " não conseguiu encontrar o arquivo urls.txt.");
            }

            sleep();
            userEntry = x.nextLine();
            ArrayList<String> urls;
            if (userEntry.contains(" ")) {
                urls = new ArrayList<String>(Arrays.asList(userEntry.split(" ")));
            } else {
                urls = new ArrayList<>();
                urls.add(userEntry);
            }
            senderQueue.add(urls);
            logger.info("Cliente" + thisUser.toString() + " add a mensagem na fila senderQueue. Renomeando arquivo");

            boolean b = file.renameTo(new File(clientFolder.getPath() + "/urls_processed.txt"));

            if(!b) {
                logger.warning("Cliente" + thisUser.toString() + " não conseguiu renomear o arquivo");
                throw new RuntimeException("Unable to rename file");
            } else {
                receive();
            }
        }
    }

    public void process() {
        while (true) {
            sleep();
            Message m = processQueue.poll();
            if (m != null) {
                processMessage(m);
            }
        }
    }

    public void processMessage(Message m) {
        LinkedTreeMap<String, Set<String>> pointedLinks
                = (LinkedTreeMap<String, Set<String>>) m.getContent();
        logger.info("Cliente" + thisUser + " mensagem sendo processada");

        Gson g = new Gson();
        String pointedLinksString = g.toJson(pointedLinks);
        JsonParser parser = new JsonParser();
        JsonObject json = parser.parse(pointedLinksString).getAsJsonObject();
        g = new GsonBuilder().setPrettyPrinting().create();
        String prettyPointedLinks = g.toJson(json);

        Path file = Paths.get(clientFolder.getPath() + "/inverted_links.txt");
        try {
            Files.write(file, Collections.singleton(prettyPointedLinks), StandardCharsets.UTF_8);
        } catch (Exception e) {
            logger.warning("Cliente" + thisUser + " não conseguiu instanciar arquivo de resposta.");
            throw new RuntimeException("Unable to create answer file.");
        }
    }

    private void cleanBuffer(byte[] buffer) {
        for (int i = 0; i < buffer.length; i++) {
            buffer[i] = 0;
        }
    }

    private void sleep() {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
}
