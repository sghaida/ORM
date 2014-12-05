using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ORM.Exceptions
{
    public class NoTableIDFieldException : System.Exception
    {
        public override string Message
        {
            get
            {
                return "No class property was marked as an ID Field with the attribute: [IsIDField], in the class. Kindly revise the class definition.";
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

        public NoTableIDFieldException() : base() { }

        public NoTableIDFieldException(string className) : base() { this.Source = className; }

    }
}
