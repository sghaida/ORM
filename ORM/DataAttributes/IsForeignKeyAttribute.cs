using System;

namespace ORM.DataAttributes
{
    /// <summary>
    ///     This attribute tells the Repository that it's associated property resembles a Database Table Foreign Key.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class IsForeignKeyAttribute : Attribute
    {
        public IsForeignKeyAttribute(bool status = true)
        {
            Status = status;
        }

        public bool Status { get; private set; }
    }
}