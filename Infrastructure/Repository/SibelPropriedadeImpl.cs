using System;
using System.Data;
using Dapper;
using IntegratorNet.Domain.Repository;
using IntegratorNet.Infrastructure.OracleConfigs;
using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;

namespace IntegratorNet.Infrastructure.Repository
{
    public class SibelPropriedadeImpl : ISibelPropriedade
    {
        IConfiguration _configuration;

        public SibelPropriedadeImpl(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public object GetPropriedade()
        {
            object result = null;
            try
            {
                var dyParam = new OracleDynamicParameters();

                var conn = this.GetConnection();
                if(conn.State == ConnectionState.Closed) conn.Open();

                if(conn.State == ConnectionState.Open) {
                    var query = "SELECT CD_PROPRIEDADE_SIEBEL,CD_PROPRIEDADE,TIPO_PROPRIEDADE,ORGID_CPI_ASSOCIADA,IND_STATUS, " +
                                "IND_SITUACAO,IND_DISPONIB_LOCACAO,CATEGORIA_PROPRIEDADE,IND_ALERTA,CLASSIFICACAO_ABC,ABL, " +
                                "VL_CRD,TIPO_MATERIAL,TIPO_ESTRUTURA,PERIODO_VENDA,TIPO_VENDA,QTD_COMERCIALIZADA,DT_ULT_UPD_VALORES, " +
                                "ULTIMO_ALUGUEL_FATUR,ULTIMO_ALUGUEL_PERCENT,ULTIMO_ALUGUEL_MINIMO,ULTIMO_ALUGUEL_CONTRATO, " +
                                "ULTIMO_CONDOMINIO,ULTIMO_FUNDO_PROMO,ULTIMA_ATIVIDADE,ULTIMO_LOJISTA,DT_SAIDA_LOJISTA, " +
                                "AMM_TOTAL_PRIMEIRO_ANO_VIAB,AMM_TOTAL_VIAB,ALUGUEL_M2_PRIMEIRO_ANO_VIAB,PRIMEIRO_ANO_VIAB, " +
                                "CDU_M2_VIAB,CDU_VIAB,NUM_ALUGUEIS_ANO_VIAB,CONDOMINIO_VIAB,VGL_VIAB,PREVISAO_SEGMTO_VIAB, " +
                                "ALUGUEL_M2_PRIMEIRO_ANO_TAB,ALUGUEL_PRIMEIRO_ANO_TAB,CDU_M2_TAB,CDU_TAB,VGL_TAB,SETOR_LOCALIZACAO, " +
                                "PAVIMENTO_LOCALIZACAO,CD_CPI_ASSOCIADA,DT_INSERT,DT_UPDATE,IND_RESERVA_TECNICA,DT_INI_REFORMA, " +
                                "DT_FIM_REFORMA from BI_STG.STG_CRM_PROPRIEDADE";
                    result = SqlMapper.Query(conn,query,param: dyParam, commandType:CommandType.StoredProcedure);
                }
            }
            catch (Exception){
                throw;
            } 
            
            return result;
        }

        public IDbConnection GetConnection(){
            var connectionString = _configuration.GetSection("ConnectionStrings").GetSection("IntegratorConnection").Value;
            var conn = new OracleConnection(connectionString);           
            return conn;
        }
    }
}