using System.Data.Common;
using System.Collections.Generic;
using System.Data;
using IntegratorCore.Cmd.Domain.Repository;
using IntegratorCore.Cmd.Domain.Entities;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Linq;

namespace IntegratorCore.Cmd.Infrastructure.Repository
{
    public class SibelSegmtoAbrasceImpl : IGenerateData<SibelSegmtoAbrasceOracle>
    {
        private readonly string _connectionStringMySQL = @"Server=brmallsapi.mysql.database.azure.com; Database=federationsiebel;Uid=developer@brmallsapi;Password=integration$$22@!;";        
        private readonly string _connectionStringOracle = @"Data Source=localhost:1521/BRMBIPRD;User Id =bi_read;Password=bi#brmalls#26;";       
        private readonly string _dmlSelect = @"SELECT Cd_Segmto_Abrasce, Nm_Segmto_Abrasce from BI_STG.STG_CRM_SEGMTO_ABRASCE";
        private readonly string _dmlInsert = @"INSERT INTO clog_crm_segmto_abrasce (EVENTO, DT_EVENTO,Cd_Segmto_Abrasce, Nm_Segmto_Abrasce)" ;

        public void GetResult()
        {
            //TODO: Alterar ADO pelo Dapper hidratando uma Entidade
            DataTable table = new DataTable();
            using (OracleConnection connection = new OracleConnection(_connectionStringOracle))
            {
                connection.Open();                                
                using (OracleDataAdapter adapter = new OracleDataAdapter(_dmlSelect, connection))
                {
                    adapter.Fill(table);
                };
                connection.Close();
            }                
            // Getting datatable layout from database
            IList<SibelSegmtoAbrasceOracle> _sibelSegmtoAbrasce = new List<SibelSegmtoAbrasceOracle>();
            if (table != null && table.Rows.Count > 0)
            {                                
                for(int i = 0; i < table.Rows.Count; i++)
                {
                    var row = table.Rows[i];
                    _sibelSegmtoAbrasce.Add(
                        new SibelSegmtoAbrasceOracle(
                            (string)row["Cd_Segmto_Abrasce"].ToString(),
                            (string)row["Nm_Segmto_Abrasce"].ToString()
                        )
                    );                    
                }
                BulkInsertMySQL(_sibelSegmtoAbrasce);
            }
        }

        public void GetResult(string sql, List<SibelSegmtoAbrasceOracle> generic)
        {
            throw new NotImplementedException();
        }

        private void BulkInsertMySQL(IList<SibelSegmtoAbrasceOracle> results)
        { //TODO: Alterar o ADO pelo Dapper.
          //TODO: Alterar para respeitar mesmo a quantidade de dados. Aqui ele está fazendo um por um.          
            try
            {
                
                if(results != null && results.Count > 0)
                {
                    //Configura a Paginação
                    int total = results.Count; //Pega o total de resultados na lista
                    int pageSize = 1000; //Quantidade de registros que vamos processar                    
                    int totalPages = total / pageSize; //Baseado no total e nos registros, qual o total de páginas para iterar
                    using (MySqlConnection connection = new MySqlConnection(_connectionStringMySQL)) 
                    {                    
                        connection.Open();

                        //Pega a página de processamento atual e começa a iteração
                        for(int page = 0; page <= totalPages; page++)
                        {
                            //Lista Paginada
                            var paginatedResults = results
                            .Skip(pageSize * (page))
                            .Take(pageSize)
                            .ToList(); 

                            //Abre a Transação para a quantidade atual paginada
                            using (MySqlTransaction tran = connection.BeginTransaction(IsolationLevel.Serializable))
                            {
                                //Executa a operação com o total de linhas desta página
                                foreach(var linha in paginatedResults)
                                {                                    
                                    using (MySqlCommand cmd = new MySqlCommand())
                                    {
                                        cmd.Connection = connection;
                                        cmd.Transaction = tran;
                                        cmd.CommandText = _dmlInsert + "VALUES" +
                                        $" ('{"Insert"}','{DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss")}','{linha.CdSegmtoAbrasce}','{linha.NmSegmtoAbrasce}')";
                                        cmd.ExecuteNonQuery();
                                    }                                
                                }                                
                                tran.Commit();
                            }                                                    
                        } 
                        connection.Close();                   
                    }                                                                           
                }                             
            }
            catch (Exception)
            {                
                throw;
            }            
        }
    }
}