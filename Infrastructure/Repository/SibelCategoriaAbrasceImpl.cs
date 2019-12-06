using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using System.Data;
using Dapper;
using IntegratorNet.Domain.Entities;
using IntegratorNet.Domain.Repository;
using IntegratorNet.Infrastructure.OracleConfigs;
using Microsoft.Extensions.Configuration;
using Oracle.ManagedDataAccess.Client;
using System.Linq;
using System.Transactions;

namespace IntegratorNet.Infrastructure.Repository
{
    public class SibelCategoriaAbrasceImpl : ISibelCategoriaAbrasce
    {
        List<SibelCategoriaAbrasce> categoriaAbrasces;
        const int pageSize = 1000;
        IConfiguration _configuration;

        public SibelCategoriaAbrasceImpl(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public object GetCategoriaAbrasce()
        {
            object result = null;
            try
            {
                var dyParam = new OracleDynamicParameters();

                var conn = this.GetConnection();
                if(conn.State == ConnectionState.Closed) conn.Open();

                if(conn.State == ConnectionState.Open) {
                    var query = "select cd_categoria_abrasce, nm_categoria_abrasce from BI_STG.STG_CRM_CATEGORIA_ABRASCE";
                    result = SqlMapper.Query(conn,query,param: dyParam, commandType:CommandType.StoredProcedure);
                }
            }
            catch (Exception){
                throw;
            } 
            
            return result;
        }

        public void UpdateCategoriaAbrasce(){
            var dyParam = new OracleDynamicParameters();
            var conn = this.GetConnection();
            object result = null;

            var listaPaginadaParaBulk = categoriaAbrasces.Skip((page ?? 0) * pageSize).Take(pageSize).ToList();

            foreach (var item in listaPaginadaParaBulk)
            {
                using (var transactionScope = new TransactionScope()) {
                    if(conn.State == ConnectionState.Open) {
                    var query = "UPDATE STG_CRM_CATEGORIA_ABRASCE SET " + 
                    "nm_categoria_abrasce = item.NmCategoriaAbrasce, " + 
                    "WHERE cd_categoria_abrasce = item.CdCategoriaAbrasce from BI_STG.STG_CRM_CATEGORIA_ABRASCE";
                    conn.Execute(query);
                    }
                    transactionScope.Complete(); 
                }
            }
        }

        public IDbConnection GetConnection(){
            var connectionString = _configuration.GetSection("ConnectionStrings").GetSection("IntegratorConnection").Value;
            var conn = new OracleConnection(connectionString);           
            return conn;
        }
    }
}