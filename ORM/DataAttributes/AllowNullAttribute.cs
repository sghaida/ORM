using System;

namespace ORM.DataAttributes
{
    /// <summary>
    ///     This attribute tells the Repository that it's associated property resembles a Database Column that is allowed to be
    ///     set to NULL in the corresponding table.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class AllowNullAttribute : Attribute
    {
        public AllowNullAttribute(bool status = true)
        {
            Status = status;
        }

        public bool Status { get; private set; }
    }
}