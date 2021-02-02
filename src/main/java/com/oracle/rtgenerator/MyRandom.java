package com.oracle.rtgenerator;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;
import com.github.javafaker.PhoneNumber;
import oracle.sql.NUMBER;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonValue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

public class MyRandom {
	private final java.util.concurrent.ThreadLocalRandom random = ThreadLocalRandom.current();
	final static String[] firstNames;
	final static String[] lastNames;
	final static String[] specialInstructions;
	final static String[] phoneTypes;
	private final Faker faker = new Faker(Locale.US);
	final static Product[] products;

	static {
		firstNames = initialize("first_names.txt");
		lastNames = initialize("last_names.txt");
		specialInstructions = new String[]{"Surface Mail", "Next Day Air", "Courier", "Ground", "Air Mail", "Hand Carry", "Counter to Counter", "COD", "Expidite", "Priority Overnight"};
		phoneTypes = new String[]{"Office", "Mobile","Home"};
		products = initializeProducts("movies.csv");
	}

	private static Product[] initializeProducts(String fileName) {
		final List<Product> d = new ArrayList<>();

		BufferedReader reader;
		try {
			reader = new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName), StandardCharsets.UTF_8));
			String line;
			final OracleJsonFactory factory = new OracleJsonFactory();
			while ((line = reader.readLine()) != null) {
				d.add(Product.getInstance(line, factory));
			}
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return d.toArray(new Product[d.size()]);
	}

	private static String[] initialize(String fileName) {
		final List<String> d = new ArrayList<>();

		BufferedReader reader;
		try {
			reader = new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName), StandardCharsets.UTF_8));
			String line;
			while ((line = reader.readLine()) != null) {
				d.add(String.format("%s%s", line.substring(0, 1).toUpperCase(), line.substring(1)));
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return d.toArray(new String[d.size()]);
	}

	public String randomFirstName() {
		return firstNames[random.nextInt(firstNames.length)];
	}

	public String randomLastName() {
		return lastNames[random.nextInt(lastNames.length)];
	}

	public int nextGaussian(double multiplier) {
		return Math.abs((int)Math.round(multiplier * random.nextGaussian()));
	}

	public int nextInt(int max) {
		return random.nextInt(0,max);
	}

	public String randomSpecialInstruction() {
		return specialInstructions[random.nextInt(specialInstructions.length)];
	}

	public Address randomAddress() {
		return faker.address();
	}

	public String randomPhoneType() {
		return phoneTypes[random.nextInt(phoneTypes.length)];
	}

	public PhoneNumber randomPhoneNumber() {
		return faker.phoneNumber();
	}

	public boolean randomBoolean() {
		return random.nextBoolean();
	}

	public Product randomProduct() {
		int productId = random.nextInt(100);
		if(productId < 50) {
			productId = random.nextInt(5);
		} else
		if(productId < 70) {
			productId = 5+random.nextInt(5);
		} else {
			productId = random.nextInt(products.length);
		}

		return products[productId];
	}

	public static class Product {
		public final String name;
		public final OracleJsonValue price;
		public final OracleJsonValue code;

		public Product(String name, OracleJsonValue price, OracleJsonValue code) {
			this.name = name;
			this.price = price;
			this.code = code;
		}

		public static Product getInstance(String line, OracleJsonFactory factory) throws SQLException {
			final String[] items = line.split(";");

			return new Product(items[0],
					factory.createValue(new NUMBER( Double.parseDouble(items[1]))),
					factory.createValue(new NUMBER( Long.parseLong(items[2]))));
		}
	}
}
