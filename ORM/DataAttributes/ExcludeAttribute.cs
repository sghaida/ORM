using System;

namespace ORM.DataAttributes
{
    /// <summary>
    ///     This attribute tells the Repository that it's associated property (DbColumn) can be excluded on Select, Insert, or
    ///     Update or on all of them.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class ExcludeAttribute : Attribute
    {
        public bool OnSelect { get; set; }
        public bool OnInsert { get; set; }
        public bool OnUpdate { get; set; }
    }
}