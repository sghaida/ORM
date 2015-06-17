using System;

namespace ORM.DataAttributes
{
    /// <summary>
    ///     This attribute tells the Repository that it's associated property resembles a Data Relation of source_key ->
    ///     destination_key.
    ///     It is a data relation because the "DataSources" of the different data models might be different than each other,
    ///     but he repository will manage to join the data together.
    ///     The SourceDataModel is basically a class name that belongs in the DataModels namespace.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class DataRelationAttribute : Attribute
    {
        /// <summary>
        ///     Relation Descriptive Name
        /// </summary>
        private string _name = string.Empty;

        /// <summary>
        ///     The data model type this relation is associated with
        /// </summary>
        public Type WithDataModel { get; set; }

        /// <summary>
        ///     The data modle key this relation is defined on
        /// </summary>
        public string OnDataModelKey { get; set; }

        /// <summary>
        ///     The class instance field name that shares the relation with the destination data model
        /// </summary>
        public string ThisKey { get; set; }

        /// <summary>
        ///     This sets the relation type between the two data models.
        ///     It can be one of the following options:
        ///     * UNION: The union of two data models. Equivalent to an SQL OUTER JOIN.
        ///     * INTERSECTION: The intersection of two data models. Equivalent to an SQL INNER JOIN.
        /// </summary>
        public Globals.DataRelation.Type RelationType { get; set; }

        public string Name
        {
            get
            {
                if (string.IsNullOrEmpty(_name))
                {
                    //Sample: countryid_country_countryid
                    _name = String.Format("{0}_{1}_{2}", ThisKey, WithDataModel.Name, OnDataModelKey).ToLower();
                }

                return _name;
            }
        }
    }
}