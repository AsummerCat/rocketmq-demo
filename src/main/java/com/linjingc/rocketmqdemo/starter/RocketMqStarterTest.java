package com.linjingc.rocketmqdemo.starter;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.apache.rocketmq.spring.core.RocketMQReplyListener;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * start的发送接收消息
 */
public class RocketMqStarterTest {
	@Resource
	private RocketMQTemplate rocketMQTemplate;

	@Resource(name = "extRocketMQTemplate")
	private RocketMQTemplate extRocketMQTemplate;

	/**
	 * 发送消息测试
	 * 消息不保证不重复 需要客户端解决幂等性问题
	 */
	public void producerTest() {
		//返回结果
		SendResult sendResult;
		//定义一个topic
		String topic = "user-topic";

		//发送同步消息 需要注意的可能会存在重试的情况 需要解决重复的问题 默认2次
		sendResult = rocketMQTemplate.syncSend(topic, "Hello, World!");

		//发送普通消息 需要注意的可能会存在重试的情况 需要解决重复的问题 默认2次
		sendResult = rocketMQTemplate.syncSend(topic, MessageBuilder.withPayload("Hello, World! I'm from spring message").build());

		//发送普通序列化消息 需要注意的可能会存在重试的情况 需要解决重复的问题 默认2次
		sendResult = rocketMQTemplate.syncSend(topic, new User().setUserAge((byte) 18));

		//发送普通消息 带超时时间 默认2次
		sendResult = rocketMQTemplate.syncSend(topic, "Hello, World!", 10);

		//发送序列化消息 转换成json发送 需要注意的可能会存在重试的情况 需要解决重复的问题 默认2次
		sendResult = rocketMQTemplate.syncSend(topic, MessageBuilder.withPayload(
				new User().setUserAge((byte) 21).setUserName("Lester")).setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE).build());

		//发送单向消息
		rocketMQTemplate.sendOneWay("topic-name", "send one-way message");

		//发送异步消息 并且回调
		rocketMQTemplate.asyncSend(topic, new User().setUserAge((byte) 18), new SendCallback() {
			@Override
			public void onSuccess(SendResult var1) {
				System.out.printf("发送成功=%s %n", var1);
			}

			@Override
			public void onException(Throwable var1) {
				System.out.printf("发送失败=%s %n", var1);
			}
		});


		//发送带tag的消息 组装tag的格式:  topic:tag
		rocketMQTemplate.convertAndSend(topic + ":tag0", "I'm from tag0");


		//批量发送消息 设置不同的KEYS
		List<Message> msgs = new ArrayList<Message>();
		for (int i = 0; i < 10; i++) {
			msgs.add(MessageBuilder.withPayload("Hello RocketMQ Batch Msg#" + i).
					setHeader(RocketMQHeaders.KEYS, "KEY_" + i).build());
		}

		SendResult sr = rocketMQTemplate.syncSend(topic, msgs, 60000);
		System.out.printf("--- Batch messages send result :" + sr);


		//发送消息,并且接收客户端的回复String类型
		String replyString = rocketMQTemplate.sendAndReceive(topic, "request string", String.class);


		//发送消息,并且接收客户端的回复User类型  并且根据 key进行顺序发送
		User requestUser = new User().setUserAge((byte) 9).setUserName("requestUserName");
		User replyUser = rocketMQTemplate.sendAndReceive(topic, requestUser, User.class, "order-id");


		/**
		 * 事务管理器 事务发送者 需要配置rabbitMq事务管理器
		 */
		//使用事务发送者发送普通消息 需要注意的可能会存在重试的情况 需要解决重复的问题
		sendResult = extRocketMQTemplate.syncSend(topic, MessageBuilder.withPayload("Hello, World!2222".getBytes()).build());

		//使用事务发送者发送事务消息
		Message msg = MessageBuilder.withPayload("extRocketMQTemplate transactional message " + 5).
				setHeader(RocketMQHeaders.TRANSACTION_ID, "KEY_" + 5).build();
		sendResult = extRocketMQTemplate.sendMessageInTransaction(
				topic, msg, null);
		System.out.printf("------ExtRocketMQTemplate send Transactional msg body = %s , sendResult=%s %n",
				msg.getPayload(), sendResult.getSendStatus());
	}


	/**
	 * **********************************************************************
	 * ********************客户端接收的方式**********************************
	 * **********************************************************************
	 * 实现类:
	 * RocketMQReplyListener :  接收并回复
	 * RocketMQListener: 接收没回复
	 * RocketMQPushConsumerLifecycleListener: 可以设置消费的起点位置
	 *
	 * @RocketMQMessageListener 注解中可以加入很多参数 详情点击
	 *  设置接收顺序: :consumeMode 并发还是顺序
	 *  设置接收模型: messageModel 集群还是广播
	 */

	/**
	 * 接收 top为XX 分组为XX 路由规则为tagA的消息 用字节接收并回复
	 * RocketMQReplyListener
	 * 并回复
	 */
	@Service
	@RocketMQMessageListener(topic = "${demo.rocketmq.bytesRequestTopic}", consumerGroup = "${demo.rocketmq.bytesRequestConsumer}", selectorExpression = "${demo.rocketmq.tag}")
	public class ConsumerWithReplyBytes implements RocketMQReplyListener<MessageExt, byte[]> {

		@Override
		public byte[] onMessage(MessageExt message) {
			System.out.printf("------- ConsumerWithReplyBytes received: %s \n", message);
			return "reply message content".getBytes();
		}

	}

	/**
	 * 接收String消息
	 * RocketMQListener
	 */
	@Service
	@RocketMQMessageListener(topic = "${demo.rocketmq.topic}", consumerGroup = "string_consumer", selectorExpression = "${demo.rocketmq.tag}")
	public class StringConsumer implements RocketMQListener<String> {
		@Override
		public void onMessage(String message) {
			System.out.printf("------- StringConsumer received: %s \n", message);
		}
	}


	/**
	 * 接收消息
	 * 设置消费者 消费数据的开始时间
	 * RocketMQPushConsumerLifecycleListener
	 */
	@Service
	@RocketMQMessageListener(topic = "${demo.rocketmq.msgExtTopic}", selectorExpression = "tag0||tag1", consumerGroup = "${spring.application.name}-message-ext-consumer")
	public class MessageExtConsumer implements RocketMQListener<MessageExt>, RocketMQPushConsumerLifecycleListener {
		@Override
		public void onMessage(MessageExt message) {
			System.out.printf("------- MessageExtConsumer received message, msgId: %s, body:%s \n", message.getMsgId(), new String(message.getBody()));
		}

		@Override
		public void prepareStart(DefaultMQPushConsumer consumer) {

			//设置消费者消费起点
			//    CONSUME_FROM_LAST_OFFSET, 从上一个偏移量消费
			//    CONSUME_FROM_FIRST_OFFSET, 从第一个补偿中消费
			//    CONSUME_FROM_TIMESTAMP, 从时间戳开始消费
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
			//表示表示从现在开始消费
			consumer.setConsumeTimestamp(UtilAll.timeMillisToHumanString3(System.currentTimeMillis()));
		}
	}

	/**
	 * 接收消息并且回复给客户端消息
	 * RocketMQReplyListener
	 * @author 一只写BUG的猫
	 */
	@Service
	@RocketMQMessageListener(topic = "${demo.rocketmq.objectRequestTopic}", consumerGroup = "${demo.rocketmq.objectRequestConsumer}", selectorExpression = "${demo.rocketmq.tag}")
	public class ObjectConsumerWithReplyUser implements RocketMQReplyListener<User, User> {

		@Override
		public User onMessage(User user) {
			System.out.printf("------- ObjectConsumerWithReplyUser received: %s \n", user);
			User replyUser = new User();
			replyUser.setUserAge((byte) 10);
			replyUser.setUserName("replyUserName");
			return replyUser;
		}
	}


}
