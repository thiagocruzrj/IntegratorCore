using System.Collections.Generic;
using System.Data;
using IntegratorCore.Cmd.Domain.Repository;
using IntegratorCore.Cmd.Domain.Entities;
using MySql.Data.MySqlClient;
using Oracle.ManagedDataAccess.Client;
using System.Linq;
using System;

namespace IntegratorNet.Infrastructure.Repository
{
    public class SibelClienteImpl : IGenerateData<SibelClienteOracle>
    {
        private readonly string _connectionStringMySQL = @"Server=brmallsapi.mysql.database.azure.com; Database=federationsiebel;Uid=developer@brmallsapi;Password=integration$$22@!;";
        private readonly string _connectionStringOracle = @"Data Source=localhost:1521/BRMBIPRD;User Id =bi_read;Password=bi#brmalls#26;";
        
        private readonly string _dmlSelect = @"SELECT CD_CLIENTE,TIPO_CLIENTE,NM_RAZAO_SOCIAL,CD_GRUPO_ECONOMICO,CD_TIPO_DOCUMENTO,TIPO_AGENCIA,FLG_BV,FLG_COMISSAO,FLG_ATND_MIDIA,DT_INSERT,DT_UPDATE,DS_SUBTIPO_CLIENTE,CD_DDD,NM_BAIRRO,NM_CIDADE,SG_ESTADO,NM_PAIS,CD_CEP FROM BI_STG.STG_CRM_CLIENTE";
        private readonly string _dmlInsert = @"INSERT INTO clog_crm_cliente(EVENTO, DT_EVENTO,CD_CLIENTE,TIPO_CLIENTE,NM_RAZAO_SOCIAL,CD_GRUPO_ECONOMICO,CD_TIPO_DOCUMENTO,TIPO_AGENCIA,FLG_BV,FLG_COMISSAO,FLG_ATND_MIDIA,DT_INSERT,DT_UPDATE,DS_SUBTIPO_CLIENTE,CD_DDD,NM_BAIRRO,NM_CIDADE,SG_ESTADO,NM_PAIS,CD_CEP)";
                
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
            //table = GetDataTableLayout("clog_crm_categoria_abrasce");
            IList<SibelClienteOracle> _sibelClientes = new List<SibelClienteOracle>();
            if (table != null && table.Rows.Count > 0)
            {                                
                for(int i = 0; i < table.Rows.Count; i++)
                {
                    var row = table.Rows[i];
                    _sibelClientes.Add(
                        new SibelClienteOracle(
                            (string)row["cd_cliente"].ToString(),
                            (string)row["tipo_cliente"].ToString(),
                            (string)row["nm_razao_social"].ToString(),
                            (string)row["cd_grupo_economico"].ToString(),
                            (string)row["cd_tipo_documento"].ToString(),
                            (string)row["tipo_agencia"].ToString(), 
                            (string)row["flg_bv"].ToString(),
                            (string)row["flg_comissao"].ToString(),
                            (string)row["flg_atnd_midia"].ToString(),
                            (string)row["dt_insert"].ToString(),
                            (string)row["dt_update"].ToString(),
                            (string)row["ds_subtipo_cliente"].ToString(),
                            (string)row["cd_ddd"].ToString(),
                            (string)row["nm_bairro"].ToString(),
                            (string)row["nm_cidade"].ToString(),
                            (string)row["sg_estado"].ToString(),
                            (string)row["nm_pais"].ToString(),
                            (string)row["cd_cep"].ToString()
                        )
                    );                    
                }
                BulkInsertMySQL(_sibelClientes);
            }
        }

        public void GetResult(string sql, List<SibelClienteOracle> generic)
        {
            throw new NotImplementedException();
        }

        private void BulkInsertMySQL(IList<SibelClienteOracle> results)
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
                                        $"('{"Insert"}','{DateTime.Now.ToString("yyyy/MM/dd HH:mm:ss")}','{linha.CdCliente}', '{linha.TipoCliente}', '{linha.NmRazaoSocial}', '{linha.CdGrupoEconomico}', '{linha.CdTipoDocumento}', '{linha.TipoAgencia}', '{linha.FlgBv}', '{linha.FlgComissao}', '{linha.FlgAtndMidia}', '{linha.DtInsert}', '{linha.DtUpdate}', '{linha.DsSubtipoCliente}', '{linha.CdDdd}', '{linha.NmBairro}', '{linha.NmCidade}', '{linha.SgEstado}', '{linha.NmPais}', '{linha.CdCep}')";
                                        //cmd.ExecuteNonQuery();
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