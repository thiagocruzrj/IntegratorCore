using System.Collections.Generic;
using System.Data;
using IntegratorCore.Cmd.Domain.Repository;
using IntegratorCore.Cmd.Domain.Entities;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Linq;

namespace IntegratorCore.Cmd.Infrastructure.Repository
{
    public class SibelCategoriaAbrasceImpl : IGenerateData<SibelCategoriaAbrasceOracle>
    {
        private readonly string _connectionStringMySQL = @"Server=brmallsapi.mysql.database.azure.com;Database=federationsiebel;Uid=developer@brmallsapi;Password=integration$$22@!;";        
        private readonly string _connectionStringOracle = @"Data Source=localhost:1521/BRMBIPRD;User Id =bi_read;Password=bi#brmalls#26;";                                    
        private readonly string _dmlSelect = @"select cd_categoria_abrasce, nm_categoria_abrasce from BI_STG.STG_CRM_CATEGORIA_ABRASCE ";
        private readonly string _dmlInsert = @"INSERT INTO clog_crm_categoria_abrasce(EVENTO, DT_EVENTO, cd_categoria_abrasce, nm_categoria_abrasce)";
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
            IList<SibelCategoriaAbrasceOracle> _sibelCategorias = new List<SibelCategoriaAbrasceOracle>();
            if (table != null && table.Rows.Count > 0)
            {                                
                for(int i = 0; i < table.Rows.Count; i++)
                {
                    var row = table.Rows[i];
                    _sibelCategorias.Add(
                        new SibelCategoriaAbrasceOracle(
                            (string)row["cd_categoria_abrasce"].ToString(),
                            (string)row["nm_categoria_abrasce"].ToString()
                        )
                    );
                }
                BulkInsertMySQL(_sibelCategorias);
            }
        }

        
        private void BulkInsertMySQL(IList<SibelCategoriaAbrasceOracle> results)
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
                                        cmd.CommandText = _dmlInsert + "values " +
                                        $"('{"Insert"}','{DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss")}','{linha.CdCategoriaAbrasce}','{linha.NmCategoriaAbrasce.Replace("\r","")}')"  ;
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