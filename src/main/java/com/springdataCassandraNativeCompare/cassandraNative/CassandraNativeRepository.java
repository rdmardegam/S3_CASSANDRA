package com.springdataCassandraNativeCompare.cassandraNative;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Repository;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.springdataCassandraNativeCompare.springData.entity.DummyItem;

import lombok.extern.log4j.Log4j2;

/**
 * @author Ramon Mardegam
 */
@Repository
@Log4j2
public class CassandraNativeRepository {

    //private CqlSession cqlSession = CassandraConfig.getCqlSession();
	
	@Autowired
    private CqlSession cqlSession;

    private static PreparedStatement selectAllStatement = null;
    private static PreparedStatement insertAllStatement = null;
    private static PreparedStatement updateAllStatement = null;
    private static PreparedStatement deleteAllStatement = null;
    
    @PostConstruct 
    private void init() {
    	insertAllStatement();
    	
    	System.out.println("========= CASSANDRA INFO ===========");
    	 try (CqlSession session = CqlSession.builder().build()) {
    	      // We use execute to send a query to Cassandra. This returns a ResultSet, which
    	      // is essentially a collection of Row objects.
    	      ResultSet rs = session.execute("select release_version from system.local");
    	      //  Extract the first row (which is the only one in this case).
    	      Row row = rs.one();

    	      // Extract the value of the first (and only) column from the row.
    	      assert row != null;
    	      String releaseVersion = row.getString("release_version");
    	      System.out.printf("Cassandra version is: %s%n", releaseVersion);
    	      
    	      Metadata metadata = session.getMetadata();
    	      System.out.printf("Connected session: %s%n", session.getName());

    	      for (Node node : metadata.getNodes().values()) {
    	        System.out.printf(
    	            "Datatacenter: %s; Host: %s; Rack: %s%n",
    	            node.getDatacenter(), node.getEndPoint(), node.getRack());
    	      }

    	      for (KeyspaceMetadata keyspace : metadata.getKeyspaces().values()) {
    	        for (TableMetadata table : keyspace.getTables().values()) {
    	          System.out.printf("Keyspace: %s; Table: %s%n", keyspace.getName(), table.getName());
    	        }
    	      }
    	      
    	      Metadata metaData = cqlSession.getMetadata();
    	      log.info("Listing available Nodes:");
              for (Node host : metaData.getNodes().values()) {
                  log.info("+ [{}]: datacenter='{}' and rack='{}'", 
                          host.getListenAddress().orElse(null),
                          host.getDatacenter(), 
                          host.getRack());
              }
              
              log.info("Listing available keyspaces:");
              for (KeyspaceMetadata meta : metaData.getKeyspaces().values()) {
            	  log.info("+ [{}] \t with replication={}", meta.getName(), meta.getReplication());
              }
    	      
    	      
    	      System.out.println("PROTOCOLO VERSION: " + session.getContext().getProtocolVersion());
    	      
    	      System.out.println("=========              ==========="); 
    	    
    	    
    	 }catch (Exception e) {
				e.printStackTrace();
			}
    	    // The try-with-resources block automatically close the session after we’re done with it.
    	    // This step is important because it frees underlying resources (TCP connections, thread
    	    // pools...). In a real application, you would typically do this at shutdown
    	    // (for example, when undeploying your webapp).
    	  
    	
    }
    
    public List<DummyItem> selectAll() {
        try {
        	
        	if (selectAllStatement == null) {
                selectAllStatement();
            }
        	
        	//System.out.println(cqlSession.getContext().getConfig());
        	log.info("INICIANDO PESQUISA");
        	List<DummyItem> response = new ArrayList<>();
            //ResultSet resultSet = cqlSession.execute(selectAllStatement.bind());
            
        	//ResultSet resultSet = cqlSession.execute("Select id, column_1, column_2   from local.dummy");
        	ResultSet resultSet = cqlSession.execute("Select  * from local.dummy");
            
            for (Row row : resultSet) {
                /*DummyItem item = new DummyItem( row.getString("num_cpf_cnpj"), row.getInt("mes_ano_lancamento"), 
                								row.getLocalDate("dat_autr"), row.getString("cod_chav_lancamento"),
                							 	row.getString("codigo_moeda_origem"), row.getDouble("valor"));*/
            	
            	DummyItem item = new DummyItem();
            	item.setNum_cpf_cnpj(row.getString("num_cpf_cnpj"));

                response.add(item);
            }


            return response;
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;

    }

    private void selectAllStatement() {
        try {
            Select selectStatement = selectFrom("local",
                    "dummy").columns("id", "column_2", "column_1").all();
            selectAllStatement = cqlSession.prepare(selectStatement.build());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    
    /************/
    private static PreparedStatement selectFilterStatement = null;
    
    //@Async//("threadPoolExecutor")
    public List<DummyItem> selectFilter(String num_cpf_cnpj ,int mes_ano_lancamento,LocalDate dataInicio, LocalDate dataFim) {
    	//log.info("CHAMANDO selectFilter - " + mes_ano_lancamento);
    	
    	//System.out.println("PROTOCOLO= " +  cqlSession.getContext().getProtocolVersion());
    	
    	List<DummyItem> response = new ArrayList<>();
    	try {
        
        	if (selectFilterStatement == null) {
        		 try {
        	            Select select = selectFrom("local","dummy")
        	            		.columns("num_cpf_cnpj",
        	            				"mes_ano_lancamento", 
        	            				"cod_chav_lancamento" ,
        	            				"dat_autr" , 
        	            				"codigo_moeda_origem" , 
        	            				"valor")
        	            		
        	            		.whereColumn("num_cpf_cnpj").isEqualTo(bindMarker())
        	            		.whereColumn("mes_ano_lancamento").isEqualTo(bindMarker())
        	            		.whereColumn("dat_autr").isGreaterThanOrEqualTo(bindMarker())
        	            		.whereColumn("dat_autr").isLessThanOrEqualTo(bindMarker())
        	            		;
        	            
        	            selectFilterStatement = cqlSession.prepare(select.build());

        	        } catch (Exception ex) {
        	            ex.printStackTrace();
        	        }
            }
        	
        	/*var time = new Random().nextInt(100-50);
        	time = time + 50;
        	Thread.sleep(time);*/
        	
        	
        	//System.out.println(cqlSession.getContext().getConfig());
        	//log.info("INICIANDO PESQUISA");
        	
        	
            
            BoundStatement boundStatement = selectFilterStatement.bind()
            								.setString(0, num_cpf_cnpj)
            								.setInt(1, mes_ano_lancamento)
            								.setLocalDate(2, dataInicio)
            								.setLocalDate(3, dataFim);
        	//BoundStatement boundStatement = selectFilterStatement.bind(num_cpf_cnpj, mes_ano_lancamento, dataInicio, dataFim);
            
            
            
            								
            ResultSet resultSet = cqlSession.execute(boundStatement);
            
            for (Row row : resultSet) {
            	DummyItem item = new DummyItem( row.getString("num_cpf_cnpj"), row.getInt("mes_ano_lancamento"), 
						row.getLocalDate("dat_autr"), row.getString("cod_chav_lancamento"),
					 	row.getString("codigo_moeda_origem"), row.getDouble("valor"));

                response.add(item);
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();

        }

    	return response;

    }
    
    
    
    @Async//("threadPoolExecutor")
    public CompletableFuture<List<DummyItem>> selectFilterAsync(String num_cpf_cnpj ,int mes_ano_lancamento, LocalDate dataInicio, LocalDate dataFim) {
    	log.info("CHAMANDO selectFilterAsync - " + mes_ano_lancamento);
    	
    	System.out.println("PROTOCOLO= " +  cqlSession.getContext().getProtocolVersion());
    	
    	List<DummyItem> response = new ArrayList<>();
    	try {
        
        	if (selectFilterStatement == null) {
        		 try {
        	            Select select = selectFrom("local","dummy")
        	            		.columns("num_cpf_cnpj",
        	            				"mes_ano_lancamento", 
        	            				"cod_chav_lancamento" ,
        	            				"dat_autr" , 
        	            				"codigo_moeda_origem" , 
        	            				"valor")
        	            		
        	            		.whereColumn("num_cpf_cnpj").isEqualTo(bindMarker())
        	            		.whereColumn("mes_ano_lancamento").isEqualTo(bindMarker())
        	            		.whereColumn("dat_autr").isGreaterThanOrEqualTo(bindMarker())
        	            		.whereColumn("dat_autr").isLessThanOrEqualTo(bindMarker())
        	            		;
        	            
        	            selectFilterStatement = cqlSession.prepare(select.build());

        	        } catch (Exception ex) {
        	            ex.printStackTrace();
        	        }
            }
        	
        	/*var time = new Random().nextInt(100-50);
        	time = time + 50;
        	Thread.sleep(time);*/
        	
        	
        	//System.out.println(cqlSession.getContext().getConfig());
        	//log.info("INICIANDO PESQUISA");
        	
        	
            
            BoundStatement boundStatement = selectFilterStatement.bind()
            								.setString(0, num_cpf_cnpj)
            								.setInt(1, mes_ano_lancamento)
            								.setLocalDate(2, dataInicio)
            								.setLocalDate(3, dataFim);
        	//BoundStatement boundStatement = selectFilterStatement.bind(num_cpf_cnpj, mes_ano_lancamento, dataInicio, dataFim);
            
            
            
            								
            ResultSet resultSet = cqlSession.execute(boundStatement);
            
            for (Row row : resultSet) {
            	DummyItem item = new DummyItem( row.getString("num_cpf_cnpj"), row.getInt("mes_ano_lancamento"), 
						row.getLocalDate("dat_autr"), row.getString("cod_chav_lancamento"),
					 	row.getString("codigo_moeda_origem"), row.getDouble("valor"));

                response.add(item);
            }
            
        } catch (Exception ex) {
            ex.printStackTrace();

        }

    	return CompletableFuture.completedFuture(response);
    }


    
    /**
     * @throws Exception *********/
    @Async("threadCassandraPoolExecutor")
    public CompletableFuture<Void> insertAsync(long id)  {
        

        	//Thread.sleep(Duration.ofSeconds(5).toMillis());
        	System.out.println(" ******CALL "+ Thread.currentThread().getName() + " - " + id);
        	
            if (insertAllStatement == null) {
                insertAllStatement();
            }
            
            /*if(id==8l) {
            	Thread.sleep(Duration.ofSeconds(5).toMillis());
            } */
            
            try {
				Thread.sleep(Duration.ofSeconds(5).toMillis());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            /*if(id%5l == 0) {
            	int a = Integer.parseInt("A");
            }*/
            
            
            
            
            String cpf = StringUtils.leftPad(Long.toString(id), 11, "0");
            
            BoundStatement boundStatement = insertAllStatement.bind(cpf,  102022, LocalDate.now(), UUID.randomUUID().toString(), "R$",  Double.valueOf(id));

            //cqlSession.executeAsync(boundStatement);
            cqlSession.execute(boundStatement);

            return CompletableFuture.completedFuture(null);

    }
    
    @Async("threadCassandraPoolExecutor")
    public CompletableFuture<Void> insertAsync(DummyItem item)  {
    	BoundStatement boundStatement = null;
    	
    	
    	/*if( (new Random().nextInt(100000 - 1 + 1) + 1) > 99990) {
    		 boundStatement = 
             		insertAllStatement.bind("231");
    		
        } else {
        	 boundStatement = 
            		insertAllStatement.bind(item.getNum_cpf_cnpj(), 
            				item.getMes_ano_lancamento(), item.getDat_autr(),
            				item.getCod_chav_lancamento(), 
            				item.getCodigo_moeda_origem(),
            				item.getValor());
            
        }*/
    	
    	
    	
    	 boundStatement = 
         		insertAllStatement.bind(item.getNum_cpf_cnpj(), 
         				item.getMes_ano_lancamento(), item.getDat_autr(),
         				item.getCod_chav_lancamento(), 
         				item.getCodigo_moeda_origem(),
         				item.getValor());
    	
            cqlSession.execute(boundStatement);

            return CompletableFuture.completedFuture(null);
    }
    
    
    
    public void insertAll(long id) {
        try {

        	System.out.println(" ****** "+ Thread.currentThread() + " - " + id);
        	
            if (insertAllStatement == null) {
                insertAllStatement();
            }
            
            String cpf = StringUtils.leftPad(Long.toString(id), 11, "0");
            
            BoundStatement boundStatement = insertAllStatement.bind(cpf,  102022, LocalDate.now(), UUID.randomUUID().toString(), "R$",  Double.valueOf(id));

            cqlSession.executeAsync(boundStatement);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
    

    private void insertAllStatement() {
        SimpleStatement insertStatement =
                insertInto("local","dummy")
                        .value("num_cpf_cnpj", bindMarker())
                        .value("mes_ano_lancamento", bindMarker())
                        .value("dat_autr", bindMarker())
                        .value("cod_chav_lancamento", bindMarker())
                        .value("codigo_moeda_origem", bindMarker())
                        .value("valor", bindMarker())
                        .build().setIdempotent(true).setConsistencyLevel(ConsistencyLevel.ONE);
        insertAllStatement = cqlSession.prepare(insertStatement);
    }

    public void updateAll(long id) {
        try {

            if (updateAllStatement == null) {
                updateAllStatement();
            }
            BoundStatement boundStatement = updateAllStatement.bind("FY", "yıldızlı"
                    , id);

            cqlSession.executeAsync(boundStatement);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    private void updateAllStatement() {
        Update updateStatement =
                update("local",
                        "dummy")
                        .setColumn("column_2", bindMarker())
                        .setColumn("column_1", bindMarker())
                        .whereColumn("id").isEqualTo(bindMarker());
        updateAllStatement =
                cqlSession.prepare(updateStatement.build());
    }

    public void deleteAll(long id) {
        try {

            if (deleteAllStatement == null) {
                deleteAllStatement();
            }
            BoundStatement boundStatement = deleteAllStatement.bind(id);

            cqlSession.executeAsync(boundStatement);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    private void deleteAllStatement() {
        Delete deleteStatement = deleteFrom("local","dummy").whereColumn("id").isEqualTo(bindMarker());
        deleteAllStatement =
                cqlSession.prepare(deleteStatement.build());
    }

}
