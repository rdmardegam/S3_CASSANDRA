package com.springdataCassandraNativeCompare.springData.entity;

import lombok.*;

import java.time.LocalDate;

import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;


/**
 * @author Ramon Mardegam
 */

@Getter
@Setter
@RequiredArgsConstructor
@AllArgsConstructor
@Table("dummy")
public class DummyItem {

    @PrimaryKeyColumn(name = "num_cpf_cnpj", ordinal = 1,  type = PrimaryKeyType.PARTITIONED)
    private String num_cpf_cnpj;
    
    @PrimaryKeyColumn(name = "mes_ano_lancamento", ordinal = 2, type = PrimaryKeyType.PARTITIONED)
    private int mes_ano_lancamento;
    
    @PrimaryKeyColumn(name = "dat_autr",  ordinal = 3, type = PrimaryKeyType.CLUSTERED)
    private LocalDate dat_autr;
    
    @PrimaryKeyColumn(name = "cod_chav_lancamento", ordinal = 4, type = PrimaryKeyType.CLUSTERED)
    private String cod_chav_lancamento;
    
    @Column("codigo_moeda_origem")
    private String codigo_moeda_origem;
    
    @Column("valor")
    private double valor;

}
