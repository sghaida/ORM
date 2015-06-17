// ReSharper disable DoNotCallOverridableMethodsInConstructor
namespace ORM.DataAccess
{
    public class DataModel
    {
        public DataModel()
        {
            Hash = GetHashCode().ToString();
        }

        public string Hash { get; set; }
    }
}