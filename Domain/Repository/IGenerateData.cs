using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace IntegratorCore.Domain.Repository
{
    public interface IGenerateData<T>
    {
        void GetResult(string sql, List<T> generic);
    }
}