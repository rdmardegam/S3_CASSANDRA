import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

public class Teste2 {

	
	public static void main(String[] args) throws IOException {

		long skip = 16;
		
		FileInputStream f = new FileInputStream(new File("G:\\WORK\\JAVA_FOLDER\\PROJETOS\\S3_Cassandra\\arquivoTeste.txt"));
		
		try (BufferedReader br = new BufferedReader(new InputStreamReader(f, StandardCharsets.UTF_8))) {
		
			br.skip(36);
			
			AtomicLong fileLine = new AtomicLong(0L);
			
			br.lines().forEach(e->
			{	
				//System.out.println(e.getBytes().length);
				/*br.skip(e.getBytes().length)*/
				
				System.out.println((fileLine.incrementAndGet()) + " - " +e);
			});
		}
		
	}
}
