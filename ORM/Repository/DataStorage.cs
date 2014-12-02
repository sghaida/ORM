using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

using ORM.Helpers;
using ORM.DataAccess;
//using ORM.DataModels;
//using ORM.DataMappers;


namespace LyncBillingBase.Repository
{
    public sealed class DataStorage
    {
        /***
         * DataStorage Repositories
         */
       

        /***
         * Singleton implementation with an attempted thread-safety using double-check locking
         * @source: http://csharpindepth.com/articles/general/singleton.aspx
         */
        // internal datastorage singleton container
        private static DataStorage _instance = null;

        // lock for thread-safety laziness
        private static readonly object _mutex = new object();

        // empty constuctor
        private DataStorage() { }

        //The only public method, used to obtain an instance of DataStorage
        public static DataStorage Instance
        {
            get
            {
                if (_instance == null)
                {
                    lock(_mutex)
                    {
                        if (_instance == null)
                        {
                            _instance = new DataStorage();
                        }
                    }
                }

                return _instance;
            }
        }
    }
}
