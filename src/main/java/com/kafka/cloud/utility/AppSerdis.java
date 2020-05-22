package com.kafka.cloud.utility;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import com.kafka.cloud.vo.User;

public class AppSerdis extends Serdes {

	static public final class UserSerde extends WrapperSerde<User> {

		public UserSerde() {
			super(new JsonSerializer<>(), new JsonDeserializer<>());
		}

	}

	static public Serde<User> User() {
		UserSerde serde = new UserSerde();
		Map<String, Object> map = new HashMap<>();
		map.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, User.class);
		serde.configure(map, false);
		return serde;
	}
}
