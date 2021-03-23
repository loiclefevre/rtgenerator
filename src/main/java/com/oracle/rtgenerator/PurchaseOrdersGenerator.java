package com.oracle.rtgenerator;

import com.github.javafaker.Address;
import oracle.jdbc.internal.OracleConnection;
import oracle.soda.*;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.soda.rdbms.impl.OracleOperationBuilderImpl;
import oracle.sql.NUMBER;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonGenerator;
import oracle.ucp.jdbc.PoolDataSource;

import java.io.ByteArrayOutputStream;
import java.io.InterruptedIOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAmount;
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

	protected Metrics metrics;

	private final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

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
				final OracleDocument[] cache = new OracleDocument[RANDOM_DOCS_PER_THREAD];
				final long[] bytesCache = new long[RANDOM_DOCS_PER_THREAD];
				final double[] amountsCache = new double[RANDOM_DOCS_PER_THREAD];
				//long bytes = 0;
				for (int i = 0; i < RANDOM_DOCS_PER_THREAD; i++) {
					final byte[] osonData = generatePurchaseOrder(amountsCache, i);
					bytesCache[i] = osonData.length;
					//bytes += osonData.length;
					cache[i] = db.createDocumentFrom(osonData);
				}

				//System.out.println("Thread " + id + ": random JSON docs generation OK (" + bytes + " in the local cache)");

				OracleConnection realConnection = (OracleConnection) c;

				int j = 0;

				final int randomModulo = Math.min(RANDOM_DOCS_PER_THREAD, RANDOM_DOCS_PER_THREAD / 2 + random.nextInt(RANDOM_DOCS_PER_THREAD / 2));

				long loadedDocuments = 0;
				long bytesSent = 0;
				double salesPrice = 0d;

				if (BATCH_SIZE == 1) {
					while (true) {
						try {
							bytesSent += bytesCache[j];
							salesPrice += amountsCache[j];
							j = ++j % randomModulo;

							loadedDocuments++;

							collection.insertAndGet(cache[j]);

							realConnection.commit(commitOptions);

							metrics.update(loadedDocuments, bytesSent, salesPrice);
						} catch (SQLException sqle) {
							try {
								c.rollback();
							} catch (SQLException ignored) {
							}

							throw sqle;
						}
					}
				}
				else {
					while (true) {
						try {

							for (int i = 0; i < BATCH_SIZE; i++) {
								// DATA can come from a simulator (this demo) or from a Kafka queue
								// or can be managed one by one (no batch ingest)
								bytesSent += bytesCache[j];
								salesPrice += amountsCache[j];
								batchDocuments.add(cache[j]);
								j = ++j % randomModulo;
							}
							loadedDocuments += BATCH_SIZE;

							collection.insertAndGet(batchDocuments.iterator());

							realConnection.commit(commitOptions);

							batchDocuments.clear();

							metrics.update(loadedDocuments, bytesSent, salesPrice);
						} catch (SQLException sqle) {
							try {
								c.rollback();
							} catch (SQLException ignored) {
							}

							throw sqle;
						}
					}
				}
			}
		} catch (SQLRecoverableException | OracleBatchException e) {
			if (!(e.getCause() instanceof InterruptedIOException || e.getCause() instanceof BatchUpdateException)) {
				e.printStackTrace();
			}
			//e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			countDownLatch.countDown();
			Thread.currentThread().interrupt();
		}
	}

	private byte[] generatePurchaseOrder(final double[] amountsCache, final int index) throws SQLException {
		out.reset();
		OracleJsonGenerator gen = factory.createJsonBinaryGenerator(out);

		final String firstName = random.randomFirstName();
		final String lastName = random.randomLastName();
		final String fullName = String.format("%s %s", firstName, lastName);
		final Instant instant = Instant.now().plusMillis(index);
		final String user = String.format("%s%s", firstName.charAt(0), lastName.substring(0, Math.min(lastName.length(), 8)).toUpperCase());
		final Address address = random.randomAddress();

		gen.writeStartObject(); // {
		//gen.write("threadid", factory.createValue(new NUMBER(id)));
		gen.write("reference", String.format("%s-%s", user, DATE_TIME_FORMATTER.format(instant)));
		gen.write("requestor", fullName);
		gen.write("user", user);
		gen.write("requestedAt", instant.atOffset(ZoneOffset.UTC));
		gen.writeStartObject("shippingInstructions");
		gen.write("name", fullName);
		gen.writeStartObject("address");
		gen.write("street", address.streetAddress());
		gen.write("city", address.cityName());
		gen.write("state", address.stateAbbr());
		gen.write("zipCode", address.zipCode());
		gen.write("country", address.country());

		gen.writeStartObject("geometry");
		gen.write("type", "Point");
		gen.writeStartArray("coordinates");
		gen.write(factory.createValue(new NUMBER(Double.parseDouble(address.longitude()))));
		gen.write(factory.createValue(new NUMBER(Double.parseDouble(address.latitude()))));
		gen.writeEnd(); // coordinates[]

		gen.writeEnd(); // geometry


		gen.writeEnd(); // address
		final int phones = random.nextInt(4);

		if (phones > 0) {
			gen.writeStartArray("phone");

			for (int i = 1; i <= phones; i++) {
				gen.writeStartObject();
				gen.write("type", random.phoneTypes[i - 1]);
				gen.write("number", i == 2 ? random.randomPhoneNumber().cellPhone() : random.randomPhoneNumber().phoneNumber());
				gen.writeEnd();
			}

			gen.writeEnd(); // phone[]
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

		double totalPrice = 0d;
		for (int i = 0; i < items; i++) {
			gen.writeStartObject();

			final MyRandom.Product product = random.randomProduct();

			gen.write("description", product.name);
			gen.write("unitPrice", product.price);
			gen.write("UPCCode", product.code);
			final int quantity = 1 + random.nextInt(4);
			gen.write("quantity", factory.createValue(new NUMBER(quantity)));

			totalPrice += quantity * product.priceRaw;

			gen.writeEnd();
		}

		amountsCache[index] = totalPrice;

		gen.writeEnd();

		gen.writeEnd();

		gen.close();

		return out.toByteArray();
	}

	public Metrics getMetrics() {
		return metrics;
	}
}
