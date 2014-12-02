using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ORM.DataAccess
{
    public class DataModel
    {
        public string hash { get; set; }

        public DataModel()
        {
            hash = this.GetHashCode().ToString();
        }
    }
}
