using System;
using System.Data;
using Dapper;
using IntegratorNet.Domain.Repository;
using IntegratorNet.Infrastructure.OracleConfigs;
using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;

namespace IntegratorNet.Infrastructure.Repository
{
    public class SibelNegociacaoImpl : ISibelNegociacao
    {
        IConfiguration _configuration;

        public SibelNegociacaoImpl(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public object GetNegociacao()
        {
            object result = null;
            try
            {
                var dyParam = new OracleDynamicParameters();

                var conn = this.GetConnection();
                if(conn.State == ConnectionState.Closed) conn.Open();

                if(conn.State == ConnectionState.Open) {
                    var query = "select CD_NEGOCIACAO,CD_NEGOCIACAO_PN,CD_NEGOCIACAO_ASSOC,FL_NEGOCIACAO_CERTIFICADA,CD_MARCA, " + 
                    "CD_CLIENTE,CD_SHOPPING,TIPO_OPORTUNIDADE,ESTAGIO,RECEITA_TOTAL,DT_EMISSAO,TIPO_PAGAMENTO,ALUGUEL_TOTAL, " +
                    "ENERGIA_TOTAL ,WISHLIST ,STATUS_FATURAMENTO ,VL_DESCONTO ,DT_INI_VIGENCIA ,DT_FIM_VIGENCIA,CATEGORIA_MIDIA, " +
                    "RAMO,CATEGORIA,DT_VENCIMENTO,DT_ASSINATURA,MOTIVO_REPROV,MOTIVO_REPROV_DOC,MOTIVO_PERDA,DT_INSERT,DT_UPDATE, " +
                    "DS_TP_ACAO_JURIDICA from BI_STG.STG_CRM_NEGOCIACAO";
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