using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Test
{
    class Class1:EAS.Data.IStructDbProvider
    {
        #region IStructDbProvider 成员

        public void Close()
        {
            throw new NotImplementedException();
        }

        public void Connect()
        {
            throw new NotImplementedException();
        }

        public void Delete<T>(System.Linq.Expressions.Expression<Func<T, bool>> func) where T : class
        {
            throw new NotImplementedException();
        }

        public void Insert<T>(T item) where T : class
        {
            throw new NotImplementedException();
        }

        public void InsertBatch<T>(IEnumerable<T> items) where T : class
        {
            throw new NotImplementedException();
        }

        public bool IsOpen
        {
            get { throw new NotImplementedException(); }
        }

        public EAS.Data.IQueryableWarp<T> Linq<T>() where T : class
        {
            throw new NotImplementedException();
        }

        public List<T> List<T>(System.Linq.Expressions.Expression<Func<T, bool>> where, int skip, int take) where T : class
        {
            throw new NotImplementedException();
        }

        public T Single<T>(System.Linq.Expressions.Expression<Func<T, bool>> where) where T : class
        {
            throw new NotImplementedException();
        }

        public void Update<T>(T item, System.Linq.Expressions.Expression<Func<T, bool>> func) where T : class
        {
            throw new NotImplementedException();
        }

        public void Update<T>(System.Linq.Expressions.Expression<Func<T, T>> updater, System.Linq.Expressions.Expression<Func<T, bool>> func) where T : class
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
