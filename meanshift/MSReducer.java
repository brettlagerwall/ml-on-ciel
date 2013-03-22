package meanshift;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.DataOutputStream;
import java.io.BufferedOutputStream;
import java.util.List;
import java.util.ArrayList;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class MSReducer implements FirstClassJavaTask {

	public static final int MAX_ITERATIONS = 20;

	private Reference[] mapperOutputs;
	private int numDimensions;
	private int numMappers;
	private double t1;
	private double t2;
	private int iteration;

	public MSReducer(Reference[] mapperOutputs, int numDimensions,
		int numMappers, double t1, double t2, int iteration) {
		this.mapperOutputs = mapperOutputs;
		this.numDimensions = numDimensions;
		this.numMappers = numMappers;
		this.t1 = t1;
		this.t2 = t2;
		this.iteration = iteration;
	}

	@Override
	public Reference[] getDependencies() {
		return mapperOutputs;
	}

	@Override
	public void invoke() throws Exception {
		System.out.println("Invoked reducer");

		List<MSCluster> allClusters = new ArrayList<MSCluster>();

		for (int i = 0; i < mapperOutputs.length; i++) {
			ObjectInputStream inputStream = new ObjectInputStream
				(new FileInputStream
				(Ciel.RPC.getFilenameForReference(mapperOutputs[i])));
			List<MSCluster> oldClusters =
				(List<MSCluster>) inputStream.readObject();
			inputStream.close();
			allClusters.addAll(oldClusters);
		}

		int i = 0;
		for (MSCluster c: allClusters) {
			ArrayList<double[]> temp = new ArrayList<double[]>();
			temp.add(c.getCentre());
			print(temp, "/tmp/data/reducer" + i + ".txt");
			i++;
		}

		//---------------------------------------------------------------------
		// Now, reduce.

		boolean isConverged = true;
		List<MSCluster> migratedClusters = new ArrayList<MSCluster>();
		for (MSCluster c: allClusters) {
			isConverged = shiftToMean(c) && isConverged;
			mergeCanopy(c, migratedClusters);
		}

		
		if (isConverged || iteration >= MAX_ITERATIONS) {
			generateFinalOutput(migratedClusters);
		}
		else {
			spawnNewIteration(migratedClusters);
		}
	}

	/*
	Forcing the centre to shift to the new centroid of the cluster.
	*/
	private boolean shiftToMean(MSCluster cluster) {
		System.out.println("Reducer: " + "Updated centre");
		return cluster.forceUpdateCentre();
	}

	/*
	If the cluster is within t1 distance of a cluster in the migratedList,
	then the centres are added together and put in both clusters.
	If the cluster is within t2 distance of a cluster in the migratedList,
	then the clusters are merged. They are not merged right away as there
	can potentially be a closer cluster and then it takes preference for the
	merge.
	*/
	private void mergeCanopy(MSCluster cluster,
		List<MSCluster> migratedClusters) {

		MSCluster closestCoveringCluster = null;
		double closestDistance = Double.MAX_VALUE;
		for (MSCluster c: migratedClusters) {
			double distance = c.getDistanceToCentre(cluster.getCentre());
			System.out.println("Reducer: Distance " + distance);
			if (distance < t1) {
				cluster.add(c.getCentre());
				c.add(cluster.getCentre());
				System.out.println("Reducer: " + "Adding centres");
			}

			if (distance < t2 && distance < closestDistance) {
				closestCoveringCluster = c;
				closestDistance = distance;
				System.out.println("Reducer: " + "Possible merge");
			}
		}

		if (closestCoveringCluster == null) {
			migratedClusters.add(cluster);
			System.out.println("Reducer: " + "Added to list");
		}
		else {
			closestCoveringCluster.merge(cluster);
			System.out.println("Reducer: " + "Merge occurring");
		}
	}


	/*
	Divides up the clusters evenly amongst the new map tasks. Then writes
	these objects to a file using an ObjectOutputStream. A mapper task is
	then spawned and passed the completede ref of the WriteableReference.
	Finally, once all of the new mapper tasks are spawned, a reducer task
	is spawned. As usual, the reducer task is dependant on the output
	references of all of the mapper tasks.
	*/
	private void spawnNewIteration(List<MSCluster> migratedClusters)
		throws Exception {

		System.out.println("New Iteration:" + (iteration + 1));	
		int vectorsPerMapperCeil =
			(int)(Math.ceil((double)migratedClusters.size() / numMappers));
		int vectorsPerMapperFloor =
			(int)(Math.floor((double)migratedClusters.size() / numMappers));
		
		List<MSCluster> outputForMapper = new ArrayList<MSCluster>();
		int i = 0;
		int count = 0;
		Reference[] mapperRefs =
			new Reference[Math.min(migratedClusters.size(), numMappers)];

		for (MSCluster c: migratedClusters) {
			if ((count < migratedClusters.size() % numMappers &&
				i == vectorsPerMapperCeil - 1) ||
				(count >= migratedClusters.size() % numMappers &&
				i == vectorsPerMapperFloor - 1)) {

				outputForMapper.add(c);

				WritableReference outRef =
					Ciel.RPC.getNewObjectFilename("mapper" + count);
				ObjectOutputStream outputStream =
					new ObjectOutputStream(outRef.open());
				outputStream.writeObject(outputForMapper);
				outputStream.close();

				mapperRefs[count] = Ciel.spawn(new MSMapper
					(outRef.getCompletedRef(), numDimensions,
					t1, t2, count, iteration + 1), null, 1)[0];
				count++;
				i = 0;
				outputForMapper = new ArrayList<MSCluster>();
			}
			else {
				outputForMapper.add(c);
				i++;
			}
		}

		Ciel.tailSpawn(new MSReducer(mapperRefs, numDimensions,
			numMappers, t1, t2, iteration + 1), null);

	}

	/*
	Writes out the final cluster centres to a Ciel file. Writes out the
	cluster bounding points to a text file. Returns a string saying it is
	complete.
	*/
	private void generateFinalOutput(List<MSCluster> migratedClusters)
		throws Exception {

		WritableReference finalOutputReference =
			Ciel.RPC.getNewObjectFilename("ms_clusters");
		DataOutputStream outputStream = new DataOutputStream
			(new BufferedOutputStream
			(finalOutputReference.open(), 1048576));

		int count = 0;
		for (MSCluster c: migratedClusters) {
			c.forceUpdateCentre();
			double[] centre = c.getCentre();
		
			for (int j = 0; j < numDimensions; j++) {
				outputStream.writeDouble(centre[j]);
			}

			print((ArrayList<double[]>)c.getBoundPoints(),
				"/tmp/data/cluster" + count + ".txt");
			count++;
		}
		Ciel.returnPlainString("Complete");
	}

	@Override
	public void setup() {
	}

	private void print(ArrayList<double[]> vectors, String path)
		throws Exception {

		File file = new File(path);
		if (!file.exists()) {
			file.createNewFile();
		}
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);

		for (int i = 0; i < vectors.size(); i++) {
			for (int j = 0; j < numDimensions; j++) {
				bw.write(vectors.get(i)[j] + " ");
			}
			bw.write("\n");
		}
		bw.close();
	}
}
