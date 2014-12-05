using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ORM.Excepsions
{
    public class NoTableFieldsException : System.Exception
    {
        public override string Message
        {
            get
            {
                return "Couldn't find any class property marked with the [DbColumn] Attribute in the class. Kindly revise the class definition.";
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

        public NoTableFieldsException() : base() { }

        public NoTableFieldsException(string className) : base() { this.Source = className; }

    }
}
