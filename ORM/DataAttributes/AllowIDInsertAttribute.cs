using System;

namespace ORM.DataAttributes
{
    /// <summary>
    ///     This attribute tells the Repository that it's associated property is most probably a Table ID Field that is allowed
    ///     to be changed and inserted into the corresponding database table.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class AllowIdInsertAttribute : Attribute
    {
        public AllowIdInsertAttribute(bool status = true)
        {
            Status = status;
        }

        public bool Status { get; private set; }
    }
}