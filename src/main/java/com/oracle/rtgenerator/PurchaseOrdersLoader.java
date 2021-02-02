package com.oracle.rtgenerator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import oracle.jdbc.OracleConnection;
import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Purchase Orders JSON documents generator and loader for Oracle Autonomous Databases.
 *
 * @author Loïc Lefèvre
 */
public class PurchaseOrdersLoader {
	public static void main(String[] args) {
		System.out.println("Starting loader...");

		if(args.length < 3) {
			System.out.println("Usage: loader <autonomous database service name> <user> <password> [wallet path: ./wallet*] [collection: purchase_orders*] [async: true|false*] [batch size: 1-50000, 10000*] [threads: 1-200, VCPUs*] [append: true*|false] [start with truncate: true|false*] [random docs generated per thread: 10-100000, 10000*]");
			System.out.println("Remark: the Autonomous database wallet must be extracted in a wallet subfolder from this directory: "+new File(".").getAbsolutePath());
			System.exit(-1);
		}

		PoolDataSource pds;
		int cores = Runtime.getRuntime().availableProcessors();

		try {
			String databaseService = args[0];
			String user = args[1];
			String password = args[2];
			String walletPath = args.length >= 4 ? args[3] : "./wallet";
			String collectionName = args.length >= 5 ? args[4] : "purchase_orders";

			PurchaseOrdersGenerator.ASYNC_COMMIT = args.length >= 6 ? Boolean.parseBoolean(args[5]) : PurchaseOrdersGenerator.ASYNC_COMMIT;
			PurchaseOrdersGenerator.BATCH_SIZE = args.length >= 7 ? Integer.parseInt(args[6]) : PurchaseOrdersGenerator.BATCH_SIZE;
			cores = args.length >= 8 ? Integer.parseInt(args[7]) : cores;
			PurchaseOrdersGenerator.APPEND_HINT = args.length >= 9 ? Boolean.parseBoolean(args[8]) : PurchaseOrdersGenerator.APPEND_HINT;
			PurchaseOrdersGenerator.TRUNCATE_FIRST = args.length >= 10 ? Boolean.parseBoolean(args[9]) : PurchaseOrdersGenerator.TRUNCATE_FIRST;
			PurchaseOrdersGenerator.RANDOM_DOCS_PER_THREAD = args.length >= 11 ? Integer.parseInt(args[10]) : PurchaseOrdersGenerator.RANDOM_DOCS_PER_THREAD;

			System.out.println("Database service: " + databaseService);
			System.out.println("Database user: " + user);
			System.out.println("SODA collection: " + collectionName);
			System.out.println("Asynchronous commit: " + PurchaseOrdersGenerator.ASYNC_COMMIT);
			System.out.println("Batch size: " + PurchaseOrdersGenerator.BATCH_SIZE);
			System.out.println("Parallel degree: " + cores);
			System.out.println("Append hint: " + PurchaseOrdersGenerator.APPEND_HINT);
			System.out.println("Truncate first: " + PurchaseOrdersGenerator.TRUNCATE_FIRST);
			System.out.println("Random JSON documents per thread local cache: " + PurchaseOrdersGenerator.RANDOM_DOCS_PER_THREAD);

			pds = initializeConnectionPool(databaseService, user, password, cores, walletPath);

			createSODACollectionIfNotExists(collectionName, pds);

			final CountDownLatch countDownLatch = new CountDownLatch(cores);

			long initialDocumentscount = 0;

			try (Connection c = pds.getConnection()) {
				try (PreparedStatement p = c.prepareStatement("select /*+ parallel(p) */ count(*) from "+collectionName+" p")) {
					System.out.print("Initializing current JSON document counter...");
					System.out.flush();
					try (ResultSet r = p.executeQuery()) {
						if (r.next()) {
							initialDocumentscount = r.getLong(1);
						}
					}
					System.out.printf("\rInitializing current JSON document counter done (%d)%n", initialDocumentscount);
				}
			}

			final ThreadGroup tg = new ThreadGroup("Generators");
			tg.setMaxPriority(Thread.NORM_PRIORITY + 2);

			final List<PurchaseOrdersGenerator> generators = new ArrayList<>();

			for (int j = 0; j < cores; j++) {
				final PurchaseOrdersGenerator generator = new PurchaseOrdersGenerator(j, pds, countDownLatch, collectionName);
				generators.add(generator);
				new Thread(tg, generator).start();
			}

			long startTime;
			long initStarttime = 0;
			while (true) {
				long temp = initialDocumentscount;
				double bytesLoadedPerSecond = 0.0d;
				double documentsLoadedPerSecond = 0.0d;

				startTime = System.currentTimeMillis();
				for (PurchaseOrdersGenerator generator : generators) {
					final Metrics metrics = generator.getMetrics();
					bytesLoadedPerSecond += metrics.bytesLoadedPerSecond;
					documentsLoadedPerSecond += metrics.documentsLoadedPerSecond;
				}
				Thread.sleep(500L - (System.currentTimeMillis() - startTime));

				double bytesLoadedPerSecond2 = 0.0d;
				double documentsLoadedPerSecond2 = 0.0d;


				startTime = System.currentTimeMillis();
				for (PurchaseOrdersGenerator generator : generators) {
					temp += generator.getLoadedDocuments();

					if (initStarttime == 0 && temp != initialDocumentscount) {
						initStarttime = System.currentTimeMillis();
					}

					final Metrics metrics = generator.getMetrics();
					bytesLoadedPerSecond2 += metrics.bytesLoadedPerSecond;
					documentsLoadedPerSecond2 += metrics.documentsLoadedPerSecond;
				}

				System.out.print("\r                                                                  ");
				System.out.printf("\rLoaded %,d JSON documents at %.1f docs/s (%.1f MB/s)", temp, (documentsLoadedPerSecond2 + documentsLoadedPerSecond) / 2.0d, ((bytesLoadedPerSecond2 + bytesLoadedPerSecond) / 2.0d) / (1024d * 1024d));
				System.out.printf(" [%.1f docs/s]", 1000.0d * ((double) (temp - initialDocumentscount) / (double) (System.currentTimeMillis() - initStarttime)));
				System.out.flush();

				Thread.sleep(500L - (System.currentTimeMillis() - startTime));
			}

//			countDownLatch.await();
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	private static void loadExamples(PoolDataSource pds) throws Exception {
		try (Connection c = pds.getConnection()) {
			c.setAutoCommit(false);

			try (PreparedStatement p = c.prepareStatement("insert into products (description,price,code) values (?,?,?)")) {

				BufferedReader reader;
				final ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
				try {
					reader = new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader().getResourceAsStream("example.json"), StandardCharsets.UTF_8));
					String line;
					while ((line = reader.readLine()) != null) {
						final PO po = mapper.readValue(line, PO.class);

						for (LineItems li : po.lineItems) {
							//System.out.println(li.getPart().Description);

							try {
								p.setString(1, li.getPart().Description);
								p.setDouble(2, li.getPart().UnitPrice);
								p.setLong(3, li.getPart().UPCCode);
								p.executeUpdate();

								c.commit();
							} catch (SQLException sqle) {
								try {
									c.rollback();
								} catch (SQLException ignored) {
								}
							}

						}
					}
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

				PrintWriter out = new PrintWriter(new BufferedOutputStream(new FileOutputStream("movies.csv")));

				try (Statement s = c.createStatement()) {
					try (ResultSet r = s.executeQuery("select description||';'||price||';'||code from products order by description")) {
						while (r.next()) {
							out.println(r.getString(1));
						}
					}
				}

				out.close();
			}
		}
	}

	private static PoolDataSource initializeConnectionPool(String connectionService, String user, String password, int cores, String walletPath) throws SQLException, IOException {
		PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
		pds.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");

		pds.setURL("jdbc:oracle:thin:@" + connectionService + "?TNS_ADMIN=" + new File(walletPath).getCanonicalPath().replace('\\', '/'));
		pds.setUser(user);
		pds.setPassword(password);
		pds.setConnectionPoolName("JDBC_UCP_POOL:" + Thread.currentThread().getName());
		pds.setInitialPoolSize(cores + 1);
		pds.setMinPoolSize(cores + 1);
		pds.setMaxPoolSize(cores + 1);
		pds.setTimeoutCheckInterval(30);
		pds.setInactiveConnectionTimeout(120);
		pds.setValidateConnectionOnBorrow(true);
		pds.setMaxStatements(20);
		pds.setConnectionProperty(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "20");

		return pds;
	}

	private static void createSODACollectionIfNotExists(final String name, final PoolDataSource pds) throws Exception {
		final Properties props = new Properties();
		props.put("oracle.soda.sharedMetadataCache", "true");
		props.put("oracle.soda.localMetadataCache", "true");

		final OracleRDBMSClient cl = new OracleRDBMSClient(props);

		try (Connection c = pds.getConnection()) {
			OracleDatabase db = cl.getDatabase(c);

			OracleCollection oracleCollection = db.openCollection(name);
			if (oracleCollection == null) {
				System.out.print("Creating SODA collection " + name + " ...");
				System.out.flush();

				db.admin().createCollection(name);
			}
			else if (PurchaseOrdersGenerator.TRUNCATE_FIRST) {
				try (Statement s = c.createStatement()) {
					System.out.print("Cleaning collection " + name + "...");
					System.out.flush();
					s.execute("truncate table " + name);
					System.out.println("done");
				}
			}
		}
	}
}
