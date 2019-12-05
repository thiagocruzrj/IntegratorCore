using System.Threading.Tasks;
using Dapper;

namespace IntegratorNetCore.Infrastucture.Repositories
{
    public interface IJobRepository
    {
        Task<int> ExecuteStoredProcedureAsync(string storedProcedureName, DynamicParameters pars, int? commandTimeout = null);
    }
}