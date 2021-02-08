package com.oracle.rtgenerator;

public class Metrics {

	private long lastBytesSent = 0l;
	private double bytesSentPerMs = 0.0d;

	private long loadedDocuments = 0l;
	private long lastLoadedDocuments = 0l;
	private double documentsLoadedPerMs = 0.0d;

	private double lastSalesPrice = 0.0d;
	private double salesPricePerMs = 0.0d;

	private double durationInMs;

	private long lastUpdateTime = 0;

	public void update(long loadedDocuments, long bytesSent, double salesPrice) {
		final long endTime = System.currentTimeMillis();
		durationInMs = (double) (endTime - lastUpdateTime);
		lastUpdateTime = endTime;

		bytesSentPerMs = (bytesSent - lastBytesSent) / durationInMs;
		lastBytesSent = bytesSent;

		this.loadedDocuments = loadedDocuments;
		documentsLoadedPerMs = (loadedDocuments - lastLoadedDocuments) / durationInMs;
		lastLoadedDocuments = loadedDocuments;

		salesPricePerMs = (salesPrice - lastSalesPrice) / durationInMs;
		lastSalesPrice = salesPrice;

		//System.out.println("Thread: "+durationInMs+", "+this.loadedDocuments+", "+ bytesSentPerMs +", "+ documentsLoadedPerMs +", "+ salesPricePerMs);
	}

	public long getTotalLoadedDocuments() {
		return loadedDocuments;
	}

	public double getBytesSentPerMs() {
		return bytesSentPerMs;
	}

	public double getDocumentsLoadedPerMs() {
		return documentsLoadedPerMs;
	}

	public double getSalesPricePerMs() {
		return salesPricePerMs;
	}
}
