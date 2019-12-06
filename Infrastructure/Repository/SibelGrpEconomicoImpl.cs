using System;
using System.Data;
using Dapper;
using IntegratorNet.Domain.Repository;
using IntegratorNet.Infrastructure.OracleConfigs;
using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;

namespace IntegratorNet.Infrastructure.Repository
{
    public class SibelGrpEconomicoImpl : ISibelGrpEconomico
    {
        IConfiguration _configuration;

        public SibelGrpEconomicoImpl(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public object GetGrpEconomico()
        {
            object result = null;
            try
            {
                var dyParam = new OracleDynamicParameters();

                var conn = this.GetConnection();
                if(conn.State == ConnectionState.Closed) conn.Open();

                if(conn.State == ConnectionState.Open) {
                    var query = "SELECT CD_GRP_ECONOMICO,NM_GRP_ECONOMICO,RAZAO_SOCIAL,TIPO_DOCUMENTO,NUM_DOCUMENTO,NM_WEBSITE," +
                                "CD_ATUACAO_MARCA,ATENDIMENTO_LOJA,ATENDIMENTO_MALL,ATENDIMENTO_MIDIA,KA_RESP_LOJA, " +
                                "KA_RESP_MALL_MIDIA,DT_INSERT,DT_UPDATE from BI_STG.STG_CRM_GRP_ECONOMICO";
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