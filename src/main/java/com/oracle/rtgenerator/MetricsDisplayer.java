package com.oracle.rtgenerator;

import java.util.Locale;

public class MetricsDisplayer {
	private final int resetComputedAfterNSeconds;
	private double allPrices;
	private long allPO;
	private double allBytes;
	private long count;


	private final long initialDocumentscount;
	private long loadedDocuments;
	private double bytesLoadedPerSecond, avgB, minB = Double.MAX_VALUE, maxB;
	private double documentsLoadedPerSecond, avgD, minD = Double.MAX_VALUE, maxD;
	private double salesPricePerSecond, avgP, minP = Double.MAX_VALUE, maxP;

	public MetricsDisplayer(long initialDocumentscount, int resetComputedAfterNSeconds) {
		this.initialDocumentscount = initialDocumentscount;
		this.resetComputedAfterNSeconds = resetComputedAfterNSeconds;
	}

	public MetricsDisplayer(long initialDocumentscount) {
		this.initialDocumentscount = initialDocumentscount;
		this.resetComputedAfterNSeconds = -1;
	}

	public void resetCurrent() {
		loadedDocuments = initialDocumentscount;
		bytesLoadedPerSecond = 0.0d;
		documentsLoadedPerSecond = 0;
		salesPricePerSecond = 0.0d;
	}

	public void resetComputed() {
		maxB = maxP = allPrices = allBytes = 0d;
		maxD = allPO = 0;
		minB = minP = Double.MAX_VALUE;
		minD = Double.MAX_VALUE;
		count = 0;
	}

	public void addMetrics(Metrics metrics) {
		loadedDocuments += metrics.getTotalLoadedDocuments();
		bytesLoadedPerSecond += metrics.getBytesSentPerMs();
		documentsLoadedPerSecond += metrics.getDocumentsLoadedPerMs();
		salesPricePerSecond += metrics.getSalesPricePerMs();
	}

	public void display() {
		if (resetComputedAfterNSeconds != -1 && count >= resetComputedAfterNSeconds) {
			resetComputed();
		}

		count++;

		computeDetailedMetrics();

		System.out.print("\r                                                                        ");
		if (false && documentsLoadedPerSecond < 0.05d) {
			System.out.printf(Locale.US, "\rLoaded %,d POs...", loadedDocuments);
		}
		else {
			System.out.printf(Locale.US, "\rLoaded %,d POs for $ %,.2f /s at %,d PO/s (%,.2f MB/s)",
					loadedDocuments,
					1000d * salesPricePerSecond,
					(long)Math.ceil(1000d * documentsLoadedPerSecond),
					1000d * bytesLoadedPerSecond / (1024d * 1024d));

			/*
			System.out.printf(Locale.US, " | $ %,.2f/%,.2f/%,.2f /s at %,d/%,d/%,d PO/s (%,.2f/%,.2f/%,.2f MB/s)",
					1000d * minP, 1000d * avgP, 1000d * maxP,
					1000 * minD, 1000 * avgD, 1000 * maxD,
					1000d * minB / (1024d * 1024d), 1000d * avgB / (1024d * 1024d), 1000d * maxB / (1024d * 1024d));

			 */
		}
		System.out.flush();
	}

	private void computeDetailedMetrics() {
		minP = Math.min(minP, salesPricePerSecond);
		maxP = Math.max(maxP, salesPricePerSecond);
		minD = Math.min(minD, documentsLoadedPerSecond);
		maxD = Math.max(maxD, documentsLoadedPerSecond);
		minB = Math.min(minB, bytesLoadedPerSecond);
		maxB = Math.max(maxB, bytesLoadedPerSecond);

		allPrices += salesPricePerSecond;
		allPO += documentsLoadedPerSecond;
		allBytes += bytesLoadedPerSecond;

		avgP = allPrices / (double) count;
		avgD = (long)(allPO / (double) count);
		avgB = allBytes / (double) count;
	}
}
