package helloworld;

import java.io.IOException;

import com.asgow.ciel.references.Reference;
import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class HelloWorld implements FirstClassJavaTask {

	public Reference[] getDependencies() {
		return new Reference[0];
	}

	public void setup() {
	}

	public void invoke() throws IOException {
		System.out.println("Running on worker.");
		Ciel.returnPlainString("Hello world!");
	}
}
