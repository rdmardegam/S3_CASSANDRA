import java.math.BigDecimal;

public class TesteClass {

	public static void main(String[] args) {
		
		Double valor = 230000D;
		
		Double aporteMensal = 2500D;
		
		int meses = 36;
		
		for (int x=0; x<meses; x++) {
			
			Double juros = (valor*0.01D);
			
			valor = valor + juros + aporteMensal; 
			
			System.out.println(valor);
		}
		
		
		System.out.println(valor);
		
	}
}
