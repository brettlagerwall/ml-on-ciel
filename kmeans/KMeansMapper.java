package kmeans;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

// Brett
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
//

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class KMeansMapper implements FirstClassJavaTask {

	public static double getSquaredDistance(double[] x, double[] y) {
		double ret = 0.0;
		for (int i = 0; i < x.length; ++i) {
			ret += (y[i] - x[i]) * (y[i] - x[i]);
		}
		return ret;
	}

	
	
	private final Reference dataPartitionRef;
	private final Reference clustersRef;
	private final int k;
	private final int numDimensions;
	private final boolean doCache;
	
	public KMeansMapper(Reference dataPartitionRef, Reference clustersRef, int k, int numDimensions, boolean doCache) {
		this.dataPartitionRef = dataPartitionRef;
		this.clustersRef = clustersRef;
		this.k = k;
		this.numDimensions = numDimensions;
		this.doCache = doCache;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[] { this.dataPartitionRef, this.clustersRef };
	}

	@Override
	public void invoke() throws Exception {

// Brett
		File file = new File("/tmp/data/finalClusters.txt");
		if (!file.exists()) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
//
		
		DataInputStream clustersIn = new DataInputStream(new BufferedInputStream(Ciel.RPC.getStreamForReference(this.clustersRef, 1048576, false, false, false), 1048576));
	
		double[][] clusters = new double[this.k][this.numDimensions];
		
		for (int i = 0; i < this.k; ++i) {
			for (int j = 0; j < this.numDimensions; ++j) {
				clusters[i][j] = clustersIn.readDouble();
			}
			//System.err.println("Cluster " + i + " " + clusters[i][0] + " " + clusters[i][1]);
		}
		
		clustersIn.close();
		
		KMeansMapperResult result = new KMeansMapperResult(this.k, this.numDimensions);
		
		LinkedList<double[]> vectors;
		boolean doRead;
		DataInputStream dataIn;
		Iterator<double[]> vectorIterator;
		
		if (this.doCache) {
			vectors = (LinkedList<double[]>) Ciel.softCache.tryGetCache("fastkmeansin", this.dataPartitionRef);
			if (vectors == null) {
				doRead = true;
				vectors = new LinkedList<double[]>();
				vectorIterator = null;
				dataIn = new DataInputStream(new BufferedInputStream((Ciel.RPC.getStreamForReference(this.dataPartitionRef, 1048576, false, true, false)), 1048576));
			} else {
				doRead = false;
				dataIn = null;
				vectorIterator = vectors.iterator();
			}
		} else {
			doRead = true;
			dataIn = new DataInputStream(new BufferedInputStream((Ciel.RPC.getStreamForReference(this.dataPartitionRef, 1048576, false, true, false)), 1048576));
			vectorIterator = null;
			vectors = null;
		}
		
		double[] currentVector = new double[this.numDimensions];

		int v = 0;
		
		long start = System.currentTimeMillis();
		try {
		
			while (true) {
				
				if (!doRead) {
					if (vectorIterator.hasNext()) {
						currentVector = vectorIterator.next();
					} else {
						break;
					}
				} else {
					for (int j = 0; j < this.numDimensions; ++j) {
						currentVector[j] = dataIn.readDouble();
					}
					if (doCache) {
						vectors.addLast(currentVector);
					}
				}
	
				int nearestCluster = -1;
				double minDistance = Double.MAX_VALUE;
				
				for (int i = 0; i < this.k; ++i) {
					double distance = getSquaredDistance(currentVector, clusters[i]);
					if (distance < minDistance) {
						nearestCluster = i;
						minDistance = distance;
					}
				}
				
				++v;
				//System.err.println("Vector " + currentVector[0] + " " + currentVector[1]);
				result.add(nearestCluster, currentVector);

// Brett
				bw.write(nearestCluster + ": ");
				for (int testI = 0; testI < numDimensions; testI++) {
					bw.write(currentVector[testI] + " ");
				}
				bw.write("\n");
//
				
				if (doRead && doCache) {
					currentVector = new double[this.numDimensions];
				}
				
			}
			
		} catch (EOFException eofe) {
			;
		}
		long finish = System.currentTimeMillis();
		System.err.println("*****>>>>> " + (doRead ? "From-disk" : "From-cache") + " loop with " + v + " vectors took " + (finish - start) + " ms");
		
		if (doRead && doCache) {
			Ciel.softCache.putCache(vectors, "fastkmeansin", this.dataPartitionRef);
		}

		if (dataIn != null) {
		    dataIn.close();
		}

// Brett
		bw.close();
//
		
		Ciel.returnObject(result);
	}
	
	@Override
	public void setup() {
		// TODO Auto-generated method stub
		
	}

	
	
}
