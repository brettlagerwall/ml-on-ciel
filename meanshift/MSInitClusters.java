package meanshift;

import java.util.List;
import java.util.ArrayList;
import java.io.BufferedInputStream;
import java.io.DataInputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class MSInitClusters implements FirstClassJavaTask {

	private Reference data;
	private int numVectors;
	private int numDimensions;

	public MSInitClusters(Reference data, int numVectors, int numDimensions) {
		this.data = data;
		this.numVectors = numVectors;
		this.numDimensions = numDimensions;
	}

	@Override
	public Reference[] getDependencies() {
		return new Reference[] {data};
	}

	@Override
	public void invoke() throws Exception {
		System.out.println("Initializing Clusters");
	
		DataInputStream inputStream = new DataInputStream
			(new BufferedInputStream((Ciel.RPC.getStreamForReference
			(data, 1048576, false, true, false)), 1048576));

		List<MSCluster> clusters = new ArrayList<MSCluster>();

		for (int i = 0; i < numVectors; i++) {
			MSCluster c = new MSCluster(numDimensions);
			double[] point = new double[numDimensions];
			for (int j = 0; j < numDimensions; j++) {
				point[j] = inputStream.readDouble();
			}
			c.add(point);
			clusters.add(c);
		}

		Ciel.returnObject(clusters);
	}

	@Override
	public void setup() {
	}
}
