using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ORM.DataAttributes
{
    [System.AttributeUsage(System.AttributeTargets.Property)]
    public class IsKeyAttribute : Attribute
    {
        public bool Status { get; private set; }

        public IsKeyAttribute(bool status = true)
        {
            this.Status = status;
        }
    }
}
