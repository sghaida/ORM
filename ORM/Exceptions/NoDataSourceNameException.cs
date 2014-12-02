using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ORM.Exceptions
{
    public class NoDataSourceNameException : System.Exception
    {
        public override string Message
        {
            get
            {
                return "Not DataSource Name was provided for the class. Kindly add a valid [DataSource(...)] Attribute to the class.";
            }
        }


        public override string Source
        {
            get
            {
                return base.Source;
            }
            set
            {
                base.Source = value;
            }
        }

        public NoDataSourceNameException() : base() { }

        public NoDataSourceNameException(string className) : base() { this.Source = className; }

    }
}
