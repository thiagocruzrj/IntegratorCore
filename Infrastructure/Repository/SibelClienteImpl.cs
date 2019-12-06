using System;
using System.Data;
using Dapper;
using IntegratorNet.Domain.Repository;
using IntegratorNet.Infrastructure.OracleConfigs;
using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;

namespace IntegratorNet.Infrastructure.Repository
{
    public class SibelClienteImpl : ISibelCliente
    {
        IConfiguration _configuration;

        public SibelClienteImpl(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public object GetCliente()
        {
            object result = null;
            try
            {
                var dyParam = new OracleDynamicParameters();

                var conn = this.GetConnection();
                if(conn.State == ConnectionState.Closed) conn.Open();

                if(conn.State == ConnectionState.Open) {
                    var query = "SELECT CD_CLIENTE,TIPO_CLIENTE,NM_RAZAO_SOCIAL,CD_GRUPO_ECONOMICO,CD_TIPO_DOCUMENTO,TIPO_AGENCIA," + 
                                "FLG_BV,FLG_COMISSAO,FLG_ATND_MIDIA,DT_INSERT,DT_UPDATE,DS_SUBTIPO_CLIENTE,CD_DDD,NM_BAIRRO," +
                                "NM_CIDADE,SG_ESTADO,NM_PAIS,CD_CEP FROM BI_STG.STG_CRM_CLIENTE";
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