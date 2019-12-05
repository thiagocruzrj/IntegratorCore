using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Dapper;
using IntegratorNetCore.Infrastucture.OracleConnection;

namespace IntegratorNetCore.Infrastucture.Repositories
{
    public class JobRepository : IJobRepository
    {
        private static readonly ConcurrentDictionary<string, string> _connStrDic = new ConcurrentDictionary<string, string>();

        public JobRepository()
        {
            Connection = CreateDbConnection();
        }
        
        public IDbConnection Connection { get; set; }

        public IDbConnection CreateDbConnection(string key = "DefaultConnection")
        {
            return new OracleConnection(GetConnectionString(key));
        }

        private string GetConnectionString(string key)
        {
            string dicKey = $"connStr_{key}";
            string connStr = "";
            if (_connStrDic.TryGetValue(dicKey, out connStr))
            {
                if (!string.IsNullOrWhiteSpace(connStr))
                    return connStr;
            }
            connStr = AppSettings.GetConnectionString(key);
            if (string.IsNullOrWhiteSpace(connStr))
            {
                throw new KeyNotFoundException($"The config item ConnectionStrings:{key} do not exists on file appsettings.json");
            }
            _connStrDic[dicKey] = connStr;
            return connStr;
        }

        public async Task<int> ExecuteStoredProcedureAsync(string storedProcedureName, DynamicParameters pars, int? commandTimeout = null)
        {
            if (string.IsNullOrWhiteSpace(storedProcedureName))
                throw new ArgumentNullException(nameof(storedProcedureName));
            return await Connection.ExecuteAsync(storedProcedureName, pars, null, commandTimeout, CommandType.StoredProcedure);
        }
    }
}