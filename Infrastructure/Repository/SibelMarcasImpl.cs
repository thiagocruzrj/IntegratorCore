using System;
using System.Data;
using Dapper;
using IntegratorNet.Domain.Repository;
using IntegratorNet.Infrastructure.OracleConfigs;
using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;

namespace IntegratorNet.Infrastructure.Repository
{
    public class SibelMarcasImpl : ISibelMarcas
    {
        IConfiguration _configuration;

        public SibelMarcasImpl(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public object GetMarcas()
        {
            object result = null;
            try
            {
                var dyParam = new OracleDynamicParameters();

                var conn = this.GetConnection();
                if(conn.State == ConnectionState.Closed) conn.Open();

                if(conn.State == ConnectionState.Open) {
                    var query = "SELECT CD_MARCA,NM_MARCA,RAZAO_SOCIAL,CD_GRUPO_ECONOMICO,TIPO_DOCUMENTO,NUM_DOCUMENTO, " +
                                "NM_WEBSITE,CD_ATUACAO_MARCA,CD_PERFIL_SOCIAL,CD_SEGMTO_ABRASCE,CD_CATEGORIA_ABRASCE, " +
                                "FLG_ANTENA,FLG_CAIXA_ELETRONICO,FLG_MIDIA,FLG_QUIOSQUE,FLG_LOJA,FLG_EVENTO, " +
                                "VL_FAIXA_METRAGEM_DE,VL_FAIXA_METRAGEM_ATE,VL_METRAGEM_MINIMA_EVENTO, " +
                                ",VL_METRAGEM_MINIMA_QUIOSQUE,ATENDIMENTO_LOJA,ATENDIMENTO_MALL,ATENDIMENTO_MIDIA, " +
                                ",KA_RESP_LOJA,KA_RESP_MALL_MIDIA,DT_INSERT,DT_UPDATE from BI_STG.STG_CRM_MARCAS";
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