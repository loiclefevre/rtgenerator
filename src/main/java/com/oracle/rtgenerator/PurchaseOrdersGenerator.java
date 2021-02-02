package com.oracle.rtgenerator;

import com.github.javafaker.Address;
import oracle.jdbc.internal.OracleConnection;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.sql.NUMBER;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonGenerator;
import oracle.ucp.jdbc.PoolDataSource;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class PurchaseOrdersGenerator implements Runnable {

	public static int BATCH_SIZE = 10000;
	public static boolean ASYNC_COMMIT = false;
	public static boolean APPEND_HINT = true;
	public static boolean TRUNCATE_FIRST = false;
	public static int RANDOM_DOCS_PER_THREAD = 10000;

	private final int id;
	private final PoolDataSource pds;
	private final CountDownLatch countDownLatch;
	private final MyRandom random;
	private final String collectionName;
	private EnumSet<OracleConnection.CommitOption> commitOptions;

	private final OracleJsonFactory factory = new OracleJsonFactory();
	private final ByteArrayOutputStream out = new ByteArrayOutputStream();

	protected transient long loadedDocuments = 0;
	protected transient double bytesLoadedPerSecond = 0d;
	protected transient double documentsLoadedPerSecond = 0d;
	protected Metrics metrics;

	public PurchaseOrdersGenerator(int id, PoolDataSource pds, CountDownLatch countDownLatch, String collectionName) {
		this.id = id;
		this.pds = pds;
		this.countDownLatch = countDownLatch;
		commitOptions = ASYNC_COMMIT ?
				EnumSet.of(
						OracleConnection.CommitOption.WRITEBATCH,
						OracleConnection.CommitOption.NOWAIT)
				:
				EnumSet.of(
						OracleConnection.CommitOption.WRITEIMMED,
						OracleConnection.CommitOption.WAIT);
		this.random = new MyRandom();
		this.metrics = new Metrics();
		this.collectionName = collectionName;
	}

	public long getLoadedDocuments() {
		return loadedDocuments;
	}

	public double getBytesLoadedPerSecond() {
		return bytesLoadedPerSecond;
	}

	public double getDocumentsLoadedPerSecond() {
		return documentsLoadedPerSecond;
	}

	public void run() {

		try {
			try (Connection c = pds.getConnection()) {
				c.setAutoCommit(false);

				final Properties props = new Properties();
				props.put("oracle.soda.sharedMetadataCache", "true");
				props.put("oracle.soda.localMetadataCache", "true");

				final OracleRDBMSClient cl = new OracleRDBMSClient(props);
				final OracleDatabase db = cl.getDatabase(c);
				final OracleCollection collection = db.openCollection(collectionName);

				final List<OracleDocument> batchDocuments = new ArrayList<>(BATCH_SIZE);

				final Map<String, String> insertOptions = new HashMap<>();
				if (APPEND_HINT) {
					insertOptions.put("hint", "append");
				}

				//System.out.println("Thread " + id + ": generating " + RANDOM_DOCS_PER_THREAD + " random JSON documents...");
				final byte[][] cache = new byte[RANDOM_DOCS_PER_THREAD][];
				long bytes = 0;
				for (int i = 0; i < RANDOM_DOCS_PER_THREAD; i++) {
					final byte[] osonData = generatePurchaseOrder();
					bytes += osonData.length;
					cache[i] = osonData;
				}

				//System.out.println("Thread " + id + ": random JSON docs generation OK (" + bytes + " in the local cache)");

				OracleConnection realConnection = (OracleConnection) c;
				while (true) {
					try {
						long bytesSent = 0;
						int j = 0;
						final long startTime = System.currentTimeMillis();

						for (int i = 0; i < BATCH_SIZE; i++) {
							// DATA can come from a simulator (this demo) or from a Kafka queue
							// or can be managed one by one (no batch ingest)
							final byte[] osonData = cache[j];
							j = ++j % RANDOM_DOCS_PER_THREAD;
							bytesSent += osonData.length;
							batchDocuments.add(db.createDocumentFrom(osonData));
						}

						collection.insertAndGet(batchDocuments.iterator(), insertOptions);

						realConnection.commit(commitOptions);

						final long endTime = System.currentTimeMillis();

						batchDocuments.clear();

						loadedDocuments += BATCH_SIZE;
						documentsLoadedPerSecond = 1000d * ((double) BATCH_SIZE / (double) (endTime - startTime));
						bytesLoadedPerSecond = 1000d * ((double) bytesSent / (double) (endTime - startTime));

						setMetrics(documentsLoadedPerSecond,bytesLoadedPerSecond);

					} catch (SQLException sqle) {
						try {
							c.rollback();
						} catch (SQLException ignored) {
						}

						throw sqle;
					}
				}
			}
		} catch (Exception e) {
			//e.printStackTrace();
			Thread.currentThread().interrupt();
		} finally {
			countDownLatch.countDown();
		}
	}

	private void setMetrics(double documentsLoadedPerSecond, double bytesLoadedPerSecond) {
		metrics.bytesLoadedPerSecond = bytesLoadedPerSecond;
		metrics.documentsLoadedPerSecond = documentsLoadedPerSecond;
	}

	private byte[] generatePurchaseOrder() {
		out.reset();
		OracleJsonGenerator gen = factory.createJsonBinaryGenerator(out);

		gen.writeStartObject(); // {

		final String firstName = random.randomFirstName();
		final String lastName = random.randomLastName();
		final String fullName = String.format("%s %s", firstName, lastName);
		final Date dateOfPO = new Date();
		final String user = String.format("%s%s", firstName.charAt(0), lastName.substring(0, Math.min(lastName.length(), 8)).toUpperCase());
		final Address address = random.randomAddress();


		gen.write("reference", String.format("%s-%2$tY%2$tm%2$td", user, dateOfPO));
		gen.write("requestor", fullName);
		gen.write("user", user);
		gen.write("requestedAt", dateOfPO.toInstant()); // TIMESTAMP binary Oracle Database
		gen.writeStartObject("shippingInstructions");
		gen.write("name", fullName);
		gen.writeStartObject("address");
		gen.write("street", address.streetAddress());
		gen.write("city", address.cityName());
		gen.write("state", address.stateAbbr());
		gen.write("zipCode", address.zipCode());
		gen.write("country", address.country());
		gen.writeEnd();
		final int phones = random.nextInt(4);

		if (phones > 0) {
			gen.writeStartArray("phone");

			for (int i = 1; i <= phones; i++) {
				gen.writeStartObject();
				gen.write("type", random.phoneTypes[i - 1]);
				gen.write("number", i == 2 ? random.randomPhoneNumber().cellPhone() : random.randomPhoneNumber().phoneNumber());
				gen.writeEnd();
			}

			gen.writeEnd();
		}


		gen.writeEnd(); // shippingInstructions

		gen.write("costCenter", String.format("A%d", 10 * (1 + random.nextInt(10))));
		if (random.nextGaussian(10d) == 2) {
			gen.writeNull("specialInstructions");
		}
		else {
			gen.write("specialInstructions", random.randomSpecialInstruction());
		}
		gen.write("allowPartialShipment", random.randomBoolean());

		final int items = 1 + random.nextInt(5);

		gen.writeStartArray("items");

		for (int i = 0; i < items; i++) {
			gen.writeStartObject();

			final MyRandom.Product product = random.randomProduct();

			gen.write("description", product.name);
			gen.write("unitPrice", product.price);
			gen.write("UPCCode", product.code);
			gen.write("quantity", factory.createValue(new NUMBER(1 + random.nextInt(4))));

			gen.writeEnd();
		}

		gen.writeEnd();

		gen.writeEnd();

		gen.close();

		return out.toByteArray();
	}

	public Metrics getMetrics() {
		return metrics;
	}
}
