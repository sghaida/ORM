using System;

namespace ORM.Exceptions
{
    public class NoTableFieldsException : Exception
    {
        public NoTableFieldsException()
        {
        }

        public NoTableFieldsException(string className)
        {
            Source = className;
        }

        public override string Message
        {
            get
            {
                return
                    "Couldn't find any class property marked with the [DbColumn] Attribute in the class. Kindly revise the class definition.";
            }
        }

        public override string Source
        {
            get { return base.Source; }
            set { base.Source = value; }
        }
    }
}