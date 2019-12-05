using System.Threading.Tasks;
using Dapper;

namespace IntegratorNetCore.Domain.Services
{
    public interface IJobService
    {
        Task<int> ExecuteStoredProcedureJobAsync(string storedProcedureName, DynamicParameters pars, int? commandTimeout = null);
    }
}