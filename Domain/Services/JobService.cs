using System.Threading.Tasks;
using Dapper;
using IntegratorNetCore.Infrastucture.Repositories;

namespace IntegratorNetCore.Domain.Services
{
    public class JobService : IJobService
    {
        private readonly IJobRepository _jobRepository;

        public JobService(IJobRepository jobRepository)
        {
            _jobRepository = jobRepository;
        }

        public async Task<int> ExecuteStoredProcedureJobAsync(string storedProcedureName, DynamicParameters pars, int? commandTimeout = null)
        {
            return await _jobRepository.ExecuteStoredProcedureAsync(storedProcedureName, pars, commandTimeout);
        }
    }
}