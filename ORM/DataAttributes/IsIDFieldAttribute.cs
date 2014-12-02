using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ORM.DataAttributes
{
    /// <summary>
    /// This attribute tells the Repository that it's associated property resembles a Database Table ID Column.
    /// </summary>

    [System.AttributeUsage(System.AttributeTargets.Property)]
    public class IsIDFieldAttribute : Attribute
    {
        public bool Status { get; private set; }

        public IsIDFieldAttribute(bool status = true) 
        {
            this.Status = status;
        }
    }
}
