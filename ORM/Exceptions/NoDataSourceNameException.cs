using System;

namespace ORM.Exceptions
{
    public class NoDataSourceNameException : Exception
    {
        public NoDataSourceNameException()
        {
        }

        public NoDataSourceNameException(string className)
        {
            Source = className;
        }

        public override string Message
        {
            get
            {
                return
                    "Not DataSource Name was provided for the class. Kindly add a valid [DataSource(...)] Attribute to the class.";
            }
        }

        public override string Source
        {
            get { return base.Source; }
            set { base.Source = value; }
        }
    }
}