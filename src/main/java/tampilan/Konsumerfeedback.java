/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package tampilan;

import java.awt.Color;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.swing.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Properties;

public class Konsumerfeedback extends JFrame {
    private JTextArea feedbackArea;
    private KafkaConsumer<String, String> consumer;
    private Thread consumerThread;
    private Connection dbConnection;

    public Konsumerfeedback() {
        setTitle("Kafka Feedback Consumer");
        setSize(500, 400);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(null);
        
        getContentPane().setBackground(new Color(28, 118, 90));

        JLabel feedbackLabel = new JLabel("User Feedback:");
        feedbackLabel.setBounds(20, 20, 150, 25);
        feedbackLabel.setForeground(Color.WHITE);
        add(feedbackLabel);

        feedbackArea = new JTextArea();
        feedbackArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(feedbackArea);
        scrollPane.setBounds(20, 50, 450, 250);
        add(scrollPane);
        
        JButton closeButton = new JButton("Close");
        closeButton.setBounds(200, 320, 100, 30);
        closeButton.addActionListener(e -> {
            stopConsumer();// Hentikan Kafka Consumer dengan aman
            dispose(); // Tutup jendela
        });
        add(closeButton);

        // Setup the consumer when the application starts
        establishDatabaseConnection();
        startConsumer();

        setVisible(true);
    }
    
    private void stopConsumer() {
        if (consumerThread != null && consumerThread.isAlive()) {
            consumerThread.interrupt(); // Hentikan thread konsumer
        }
        if (consumer != null) {
            consumer.close(); // Tutup Kafka consumer
        }else{
            consumerThread.interrupt(); // Reset status interrupt
            

        }
        feedbackArea.append("Kafka Consumer stopped successfully.\n");
    }

    private void establishDatabaseConnection() {
        try {
            String url = "jdbc:mysql://localhost:3306/last"; // Ganti dengan nama database Anda
            String user = "root"; // Ganti dengan username MySQL Anda
            String password = ""; // Ganti dengan password MySQL Anda
            dbConnection = DriverManager.getConnection(url, user, password);
            feedbackArea.append("Database connected successfully.\n");
        } catch (SQLException e) {
            feedbackArea.append("Error connecting to database: " + e.getMessage() + "\n");
        }
    }

    private void startConsumer() {
        // Kafka Consumer Configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Kafka server address
        props.put("group.id", "feedback-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest"); // Start consuming from the earliest message

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("topik-feedbackuser"));

        consumerThread = new Thread(() -> {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(1000); // Wait for messages
                    for (ConsumerRecord<String, String> record : records) {
                        String feedbackMessage = record.value();
                        feedbackArea.append("Received Feedback: " + feedbackMessage + "\n\n");
                        saveFeedbackToDatabase(feedbackMessage); // Save feedback to database
                    }
                }
            } catch (Exception ex) {
                feedbackArea.append("Kafka Konsumer Dimatikan.");
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
        });

        consumerThread.start();
        feedbackArea.append("Kafka Consumer started successfully.\n");
    }

    private void saveFeedbackToDatabase(String message) {
        try {
            // Assuming message is in a simple format: {"userId":1,"username":"John","feedback":"Great app!"}
            String[] parts = message.replace("{", "").replace("}", "").replace("\"", "").split(",");
            int userId = Integer.parseInt(parts[0].split(":")[1]);
            String username = parts[1].split(":")[1];
            String feedback = parts[2].split(":")[1];

            String query = "INSERT INTO feedback (userid, username, pesan) VALUES (?, ?, ?)";
            PreparedStatement stmt = dbConnection.prepareStatement(query);
            stmt.setInt(1, userId);
            stmt.setString(2, username);
            stmt.setString(3, feedback);

            stmt.executeUpdate();
            feedbackArea.append("Feedback saved to database.\n");
        } catch (Exception e) {
            feedbackArea.append("Error saving feedback to database: " + e.getMessage() + "\n");
        }
    }

    public static void main(String[] args) {
        new Konsumerfeedback(); // Start the application
    }
}