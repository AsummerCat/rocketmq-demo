package com.linjingc.rocketmqdemo.client;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import java.io.UnsupportedEncodingException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

/**
 * client的方式接收发送消息
 */
public class RocketMqTest {

	/**
	 * 创建生产者
	 *
	 * @return
	 */
	private DefaultMQProducer initDefaultMQProducer() {
		DefaultMQProducer producer = new DefaultMQProducer("producer_demo");
		//指定NameServer地址
		//修改为自己的
		//多个可以用";"隔开
		//producer.setNamesrvAddr("192.168.116.115:9876;192.168.116.116:9876");
		producer.setNamesrvAddr("112.74.43.136:9876");
		/*
		 * Producer对象在使用之前必须要调用start初始化，初始化一次即可
		 * 注意：切记不可以在每次发送消息时，都调用start方法
		 */
		try {
			producer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
		}
		return producer;
	}


	/**
	 * 构造普通消息结构体
	 *
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	private Message initMessage() throws UnsupportedEncodingException {
        /*
                构建消息
                参数  topic:   Message 所属的 Topic
                      tags:   可理解为对消息进行再归类，方便 Consumer 指定过滤条件在 MQ 服务器过滤
                      keys:   设置代表消息的业务关键属性，请尽可能全局唯一,
                              以方便您在无法正常收到消息情况下，可通过阿里云服务器管理控制台查询消息并补发
                              注意：不设置也不会影响消息正常收发
                      body:    Body 可以是任何二进制形式的数据， MQ 不做任何干预，
                               需要 Producer 与 Consumer 协商好一致的序列化和反序列化方式
                 */
		Message msg = new Message("TopicTest", "TagA", "keys", ("测试RocketMQ").getBytes("UTF-8"));
		return msg;
	}

	/**
	 * *****************************发送消息****************************
	 */

	/**
	 * 发送普通同步消息
	 */
	public void sendBasicMessage() throws Exception {
		DefaultMQProducer producer = initDefaultMQProducer();
		Message msg = initMessage();
		//发送同步消息
		try {
			//发送同步消息
			SendResult sendResult = producer.send(msg);
			System.out.printf("%s%n", sendResult);

		} catch (Exception e) {
			e.printStackTrace();
			// 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理
			System.out.println(new Date() + " Send mq message failed. Topic is:" + msg.getTopic());
			Thread.sleep(1000);
		}
	}


	/**
	 * 发送异步消息
	 */
	public void sendAsyncMessage() throws Exception {
		DefaultMQProducer producer = initDefaultMQProducer();
		Message msg = initMessage();
		try {
			// 异步发送消息, 发送结果通过 callback 返回给客户端。
			producer.send(msg, new SendCallback() {
				@Override
				public void onSuccess(SendResult sendResult) {
					// 消费发送成功
					System.out.println("SUCCESS信息:" + sendResult.toString());
					System.out.println("send message success. topic=" + sendResult.getRegionId() + ", msgId=" + sendResult.getMsgId());
				}

				@Override
				public void onException(Throwable throwable) {
					// 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理
					System.out.println("FAIL信息:" + throwable.getMessage());
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
			// 消息发送失败，需要进行重试处理，可重新发送这条消息或持久化这条数据进行补偿处理
			System.out.println(new Date() + " Send mq message failed. Topic is:" + msg.getTopic());
			Thread.sleep(1000);
		}
	}


	/**
	 * 单向发送
	 */
	public void sendOnewayMessage() throws Exception {
		DefaultMQProducer producer = initDefaultMQProducer();
		Message msg = initMessage();
		// 由于在 oneway 方式发送消息时没有请求应答处理，一旦出现消息发送失败，则会因为没有重试而导致数据丢失。若数据不可丢，建议选用可靠同步或可靠异步发送方式。
		producer.sendOneway(msg);
	}

	/**
	 * 发送有序消息 MessageQueueSelector 自定义规则发送数据
	 * 默认策略 有三种
	 * SelectMessageQueueByHash   根据hash
	 * SelectMessageQueueByMachineRoom 根据机房随机
	 * SelectMessageQueueByRandom   随机选择消息队列 random方法
	 */
	public void sendOrderMessage() throws Exception {
		DefaultMQProducer producer = initDefaultMQProducer();
		Message msg = initMessage();
		// 顺序发送消息。

		//根据订单号
		int orderId = 100 % 10;
		producer.send(msg, new MessageQueueSelector() {
					@Override
					public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
						// arg的值其实就是orderId
						Integer id = (Integer) o;
						// list是队列集合，也就是topic所对应的所有队列
						int index = id % list.size();
						// 这里根据前面的id对队列集合大小求余来返回所对应的队列
						return list.get(index);
					}
				}
				, orderId);
	}


	/**
	 * 发送延时消息
	 */
	public void sendDelayMessage() throws Exception {
		DefaultMQProducer producer = initDefaultMQProducer();
		Message msg = initMessage();

		// 这里设置需要延时的等级即可 3秒
		msg.setDelayTimeLevel(3);
		SendResult sendResult = producer.send(msg);
	}


	/**
	 * 发送事务消息
	 */
	public void sendTransactionMessage() throws Exception {
		//创建一个事务监听器
		TransactionListener transactionListener = new TransactionListenerImpl();
		//这里使用 事务的生产者 来发送消息
		TransactionMQProducer producer = new TransactionMQProducer("please_rename_unique_group_name");
		ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread thread = new Thread(r);
				thread.setName("client-transaction-msg-check-thread");
				return thread;
			}
		});

		producer.setExecutorService(executorService);
		producer.setTransactionListener(transactionListener);
		producer.start();

		//发送消息
		try {
			Message msg = initMessage();
			//这里使用 事务生产者发送事务信息 如果抛出异常即回滚数据
			TransactionSendResult sendResult = producer.sendMessageInTransaction(msg, null);
			System.out.printf("%s%n", sendResult);

			Thread.sleep(10);
		} catch (MQClientException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}


/**
 * *****************************接收消息****************************
 */


	/**
	 * 初始化消费者
	 *
	 * @return
	 * @throws MQClientException
	 */
	private DefaultMQPushConsumer initDefaultMQPushConsumer() throws MQClientException {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_demo");
		//指定NameServer地址，多个地址以 ; 隔开
		//consumer.setNamesrvAddr("112.74.43.136:9876;192.168.116.116:9876"); //修改为自己的
		consumer.setNamesrvAddr("112.74.43.136:9876");
		/**
		 * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费
		 * 如果非第一次启动，那么按照上次消费的位置继续消费
		 * CONSUME_FROM_LAST_OFFSET //默认策略，从该队列最尾开始消费，即跳过历史消息
		 */
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		//这里可以设置选中 你需要监听 top 和tag
		consumer.subscribe("TopicTest", "TagA || TagC || TagD");
		return consumer;
	}


	/**
	 * 接收到普通消息
	 */
	public void receiveBasicMessage() throws Exception {
		DefaultMQPushConsumer consumer = initDefaultMQPushConsumer();
		//注册监听消息
		consumer.registerMessageListener(new MessageListenerOrderly() {
			@Override
			public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
				//关闭自动提交
				consumeOrderlyContext.setAutoCommit(false);
				try {
					for (MessageExt msg : list) {
						String msgbody = new String(msg.getBody(), "utf-8");
						System.out.println("  MessageBody: " + msgbody);//输出消息内容
					}
				} catch (Exception e) {
					e.printStackTrace();
					return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT; //稍后再试
				}
				//返回消费状态
				//SUCCESS 消费成功
				//SUSPEND_CURRENT_QUEUE_A_MOMENT 消费失败，暂停当前队列的消费
				return ConsumeOrderlyStatus.SUCCESS;
			}
		});
	}


	/**
	 * 广播消费信息
	 */
	public void receiveBroadcastMessage() throws Exception {
		DefaultMQPushConsumer consumer = initDefaultMQPushConsumer();
		//设置为广播模式 默认为CLUSTERING模式
		//MessageModel.BROADCASTING用来接收广播消息
		consumer.setMessageModel(MessageModel.BROADCASTING);
		consumer.registerMessageListener(new MessageListenerOrderly() {
			@Override
			public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
				context.setAutoCommit(false);
				System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
				return ConsumeOrderlyStatus.SUCCESS;
			}
		});
	}


}
