package com.kafka.cloud.utility;

import java.util.ArrayList;
import java.util.List;

import com.kafka.cloud.vo.User;

public class DataGenerator {

	public static List<User> generateUserData() {

		List<User> userList = new ArrayList<>();
		for (int i = 0; i < AppConstants.DATA_GENERATOR_SIZE; i++) {
			User user = new User();
			user.setFirstName("Pranay" + i);
			user.setLastName("Raut" + i);
			user.setUsername("raut" + Math.random() + "pranay");
			userList.add(user);
		}
		return userList;
	}

}
