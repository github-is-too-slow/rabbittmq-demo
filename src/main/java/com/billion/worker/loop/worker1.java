package com.billion.worker.loop;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @author Billion
 * @create 2021/03/28 15:57
 */
public class worker1 {
    public static void main(String[] args) {
        new Thread(new Fanout(), "queue7").start();
    }

    static class Fanout implements Runnable{
        public void run() {
            //创建连接工厂
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connectionFactory.setHost("39.103.174.115");
            connectionFactory.setPort(5672);
            connectionFactory.setUsername("admin");
            connectionFactory.setPassword("admin");
            connectionFactory.setVirtualHost("/");
            Connection conn = null;
            Channel channel = null;
            try {
                //创建连接
                conn = connectionFactory.newConnection("新连接生产者");
                //创建通道
                channel = conn.createChannel();
                //接受消息
                String queueName = Thread.currentThread().getName();
                //自动应答autoAck=true
                channel.basicConsume(queueName, true, new DeliverCallback() {
                    public void handle(String consumerTag, Delivery message) throws IOException {
                        System.out.println("msg = " + new String(message.getBody(), "utf-8"));
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }, new CancelCallback() {
                    public void handle(String consumerTag) throws IOException {
                        System.out.println("接受消息失败");
                    }
                });
                System.out.println("开始接受消息");
                System.in.read();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                //关闭通道
                if(channel != null && channel.isOpen()){
                    try {
                        channel.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (TimeoutException e) {
                        e.printStackTrace();
                    }
                }
                //关闭连接
                if(conn != null && conn.isOpen()){
                    try {
                        conn.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        }
    }
}
