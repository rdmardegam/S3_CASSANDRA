###### Apache Cassandra | Spring Data for Cassandra - Cassandra Native Driver CRUD Performance Comparison
**Medium Link:** https://medium.com/@fatih_yildizli/apache-cassandra-spring-data-for-cassandra-cassandra-native-driver-crud-performance-comparison-97843e98e162

###### **‍🗨 Performance Ranking for CRUD 1 million row**

_**C**reate_

`Cassandra Native Driver      (19723ms) -
Spring Data for Cassandra    (3170808ms)`

_**R**ead_

`Cassandra Native Driver      (8668ms) - 
Spring Data for Cassandra    (50455ms)`

_**U**pdate_

`Cassandra Native Driver      (16691ms) - 
Spring Data for Cassandra    (3125532ms)`

_**D**elete_

`Cassandra Native Driver      (30055ms) - 
Spring Data for Cassandra    (3093708ms)`


---------------
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller
public class TesteFutureController2 {
	
	@RequestMapping(path = "/future2", method = RequestMethod.GET)
	public ResponseEntity<?> teste(Integer id_cartao_adicional) {
		System.out.println("ENTROU");
		System.out.println(id_cartao_adicional);
		
		// Pequisa ids dos cartoes... se falhar... podemos retornar apenas o id informado... devolvendo uma info errada, mas talvez aceitavel
		CompletableFuture<List<Integer>> future1 = this.pesquisaIds(id_cartao_adicional);
		
		// Buscando valores no banco de dados... se falhar... nao deve retornar erro 500 ao cliente
		CompletableFuture<String> future2 = this.getValorBancoDeDados(id_cartao_adicional);

		System.out.println("Preparando JOIN");
		CompletableFuture.allOf(future1, future2).join();
		System.out.println("JOIN FINALIZADO");
		
		String valor = "";
		
		try {
			System.out.println("Preparando GET - " + Thread.currentThread().getName());
			valor += future1.get();
			valor += future2.get() ;
			System.out.println("FIM");
			return new ResponseEntity<>(valor, HttpStatus.OK);
		} catch (Exception e) {
			return new ResponseEntity<>("ERRO INTERNO", HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	
	private CompletableFuture<List<Integer>> pesquisaIds(Integer id_cartao_adicional) {
		return CompletableFuture.supplyAsync(() -> {
			
			System.out.println("PESQUISA IDS - " + Thread.currentThread().getName());
			// Aguarda 1 segundos
			spleep(1);
			
			if (id_cartao_adicional % 2 == 0) {
				throw new CompletionException(new Exception("ERRO AO BUSCAR ID CARTAO " + 2));
			}
			// Aguarda 1 segundos
			spleep(1);

			return List.of(id_cartao_adicional, 222, 333, 444, 555);
		}).exceptionally(exception -> {
			System.out.println("ERRO AO PESQUISA LISTA DE CARTOES - RETORNANDO APENAS " + id_cartao_adicional);
			System.err.println(exception);
			return List.of(id_cartao_adicional);
		});
	}

	
	private CompletableFuture<String> getValorBancoDeDados(Integer id_cartao_adicional) {
		return CompletableFuture.supplyAsync(() -> {
			
			System.out.println("PESQUISA BASE DE DADOS - " + Thread.currentThread().getName());
			
			spleep(3);
			if (id_cartao_adicional==2) {
				throw new CompletionException(new Exception("ERRO AO BUSCAR ID CARTAO "+ 2));
			}
			System.out.println("CHAMOU GETVALOR 2");
			return "VALOR_BANCO_CASSANDRA";
		});
	}

	
	
	

	private void spleep(int sleepSeconds) {
		try {
			Thread.sleep(1000*sleepSeconds);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
  
  -----------
  
  https://www.callicoder.com/java-8-completablefuture-tutorial/
