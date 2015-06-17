using System;

namespace ORM.DataAttributes
{
    /// <summary>
    ///     This attribute tells the Repository that it's associated property resembles a Database Column and with a specific
    ///     name.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class DbColumnAttribute : Attribute
    {
        public DbColumnAttribute(string name)
        {
            Name = name;
        }

        public string Name { get; private set; }
    }
}