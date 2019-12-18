using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace IntegratorCore.Cmd.Domain.Repository
{
    public interface IGenerateData<T>
    {
        void GetResult();
    }
}