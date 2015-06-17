using System;

namespace ORM.Exceptions
{
    public class NoTableIdFieldException : Exception
    {
        public NoTableIdFieldException()
        {
        }

        public NoTableIdFieldException(string className)
        {
            Source = className;
        }

        public override string Message
        {
            get
            {
                return
                    "No class property was marked as an ID Field with the attribute: [IsIDField], in the class. Kindly revise the class definition.";
            }
        }

        public override string Source
        {
            get { return base.Source; }
            set { base.Source = value; }
        }
    }
}