package com.springdataCassandraNativeCompare.springData;

/**
 * @author Ramon Mardegam
 */


import com.springdataCassandraNativeCompare.springData.entity.DummyItem;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;
import java.util.List;

public interface SpringDataRepository extends CassandraRepository<DummyItem, MapId> {

    @Query("select * from dummy where num_cpf_cnpj= :num_cpf_cnpj "
    		+ " and mes_ano_lancamento = :mes_ano_lancamento "
    		+ " and dat_autr >= :dataInicio "
    		+ " and dat_autr <= :dataFim ;")
    List<DummyItem> selectAll2(@Param("num_cpf_cnpj") String num_cpf_cnpj,
    		@Param("mes_ano_lancamento")  int mes_ano_lancamento,
    		@Param("dataInicio")  LocalDate dataInicio, 
    		@Param("dataFim")  LocalDate dataFim);
    
    
    @Query("select * from dummy;")
    List<DummyItem> selectAll();
    
    @Query(value = "insert into dummy (num_cpf_cnpj, mes_ano_lancamento, cod_chav_lancamento , dat_autr , codigo_moeda_origem , valor)"
    				+ " VALUES (:num_cpf_cnpj , :mes_ano_lancamento , :cod_chav_lancamento , :dat_autr , :codigo_moeda_origem , :valor)")
    void insert(@Param("num_cpf_cnpj") String num_cpf_cnpj,
    			@Param("mes_ano_lancamento") int mes_ano_lancamento,
    			@Param("cod_chav_lancamento") String cod_chav_lancamento,
    			@Param("dat_autr") LocalDate dat_autr,
    			@Param("codigo_moeda_origem") String codigo_moeda_origem,
    			@Param("valor") double valor
    		);

    @Query(value = "update dummy set column_2=:column_2,column_1=:column_1 where id=:id")
    void update(@Param("id") long id, @Param("column_2") String column_2, @Param("column_1") String column_1);

    @Query("delete from dummy where id=:id")
    void delete(@Param("id") long id);
}

