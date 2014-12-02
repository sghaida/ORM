using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

//using ORM.Libs;
using ORM.DataAttributes;

namespace ORM.Helpers
{
    // Helper class for the ConverToList<T> function
    public class ObjectPropertyInfoField
    {   
        public string DataFieldName { get; set; }
        public string ObjectFieldName { get; set; }
        public PropertyInfo Property { get; set; }
        public Type DataFieldType { get; set; }
    }


    public static class Extensions
    {
        /// <summary>
        /// Converts datatable to list<T> dynamically
        /// </summary>
        /// <typeparam name="T">Class name</typeparam>
        /// <param name="dataTable">data table to convert</param>
        /// <returns>List<T></returns>
        public static List<T> ConvertToList<T>(this DataTable DataTable,bool IncludeDataRelations = true) where T : class, new()
        {
            var dataList = new List<T>();

            // List of class property infos
            List<PropertyInfo> masterPropertyInfoFields = new List<PropertyInfo>();
            List<PropertyInfo> childPropertInfoFields = new List<PropertyInfo>();
           
            Dictionary<string, List<ObjectPropertyInfoField>> childrenObjectsProperties = new Dictionary<string, List<ObjectPropertyInfoField>>();
            Dictionary<string, List<ObjectPropertyInfoField>> cdtPropertyInfo = new Dictionary<string, List<ObjectPropertyInfoField>>();

            //List of T object data fields (DbColumnAttribute Values), and types.
            List<ObjectPropertyInfoField> masterObjectFields = new List<ObjectPropertyInfoField>();

            //Define what attributes to be read from the class
            const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;

            // Initialize Master the property info fields list
            masterPropertyInfoFields = typeof(T).GetProperties(flags)
                .Where(property => property.GetCustomAttribute<DbColumnAttribute>() != null)
                .Cast<PropertyInfo>()
                .ToList();

            //Read Datatable column names and types
            var dtlFieldNames = DataTable.Columns.Cast<DataColumn>()
                .Select(item => new
                {
                    Name = item.ColumnName,
                    Type = item.DataType
                }).ToList();

            // Initialize the object data fields  list for Master Object
            foreach (var item in masterPropertyInfoFields)
            {
                masterObjectFields.Add(new ObjectPropertyInfoField
                {
                    Property = item,
                    DataFieldName = item.GetCustomAttribute<DbColumnAttribute>().Name,
                    DataFieldType = Nullable.GetUnderlyingType(item.PropertyType) ?? item.PropertyType
                });
            }


            if (IncludeDataRelations == true)
            {
                // Initialize child the property info fields list
                childPropertInfoFields = typeof(T).GetProperties(flags)
                 .Where(property => property.GetCustomAttribute<DataRelationAttribute>() != null)
                 .Cast<PropertyInfo>()
                 .ToList();


                // Fill the childrenObjectsProperties dictionary with the name of the children class for reflection and their corrospndant attributes
                foreach (PropertyInfo property in childPropertInfoFields)
                {
                    Type childtypedObject = property.PropertyType;

                    var childtableFields = childtypedObject.GetProperties(flags)
                          .Where(item => item.GetCustomAttribute<DbColumnAttribute>() != null).
                          Select(item => new ObjectPropertyInfoField
                          {
                              Property = (PropertyInfo)item,
                              DataFieldName = item.GetCustomAttribute<DbColumnAttribute>().Name,
                              DataFieldType = Nullable.GetUnderlyingType(item.PropertyType) ?? item.PropertyType
                          })
                          .ToList();

                    var tableName = childtypedObject.GetCustomAttribute<DataSourceAttribute>().Name;

                    childrenObjectsProperties.Add(tableName, childtableFields);

                }

                //Get the Children classes related columns from datatable
                foreach (KeyValuePair<string, List<ObjectPropertyInfoField>> childObjectsProperties in childrenObjectsProperties)
                {
                    var childObjectColumns = (from childObjField in childObjectsProperties.Value
                                              join dtlFieldName in dtlFieldNames on
                                              (childObjectsProperties.Key + "." + childObjField.DataFieldName) equals dtlFieldName.Name
                                              where dtlFieldName.Type == childObjField.DataFieldType
                                              select
                                              new ObjectPropertyInfoField()
                                              {
                                                  DataFieldName = dtlFieldName.Name,
                                                  DataFieldType = dtlFieldName.Type,
                                                  Property = childObjField.Property,
                                                  ObjectFieldName = childObjField.DataFieldName
                                              }).ToList();

                    cdtPropertyInfo.Add(childObjectsProperties.Key, childObjectColumns);
                }

            }

           //Fill The data
           Parallel.ForEach(DataTable.AsEnumerable().ToList(),
                (datarow) =>
                {
                    var masterObj = new T();

                    if (IncludeDataRelations == true)
                    {
                        //Fill the Data for children objects
                        foreach (PropertyInfo property in childPropertInfoFields)
                        {
                            Type childtypedObject = property.PropertyType;

                            var childObj = Activator.CreateInstance(childtypedObject);

                            List<ObjectPropertyInfoField> data;
                            cdtPropertyInfo.TryGetValue(childtypedObject.GetCustomAttribute<DataSourceAttribute>().Name, out data);

                            foreach (var dtField in data)
                            {
                                var dataField = data.Find(item => item.DataFieldName == dtField.DataFieldName);

                                if (dataField != null)
                                {
                                    PropertyInfo dataFieldPropertyInfo = dataField.Property;

                                    if (dataFieldPropertyInfo.PropertyType == typeof(DateTime))
                                    {
                                        dataFieldPropertyInfo.SetValue(childObj, datarow[dtField.DataFieldName].ReturnDateTimeMinIfNull(), null);
                                    }
                                    else if (dataFieldPropertyInfo.PropertyType == typeof(int))
                                    {
                                        dataFieldPropertyInfo.SetValue(childObj, datarow[dtField.DataFieldName].ReturnZeroIfNull(), null);
                                    }
                                    else if (dataFieldPropertyInfo.PropertyType == typeof(long))
                                    {
                                        dataFieldPropertyInfo.SetValue(childObj, datarow[dtField.DataFieldName].ReturnZeroIfNull(), null);
                                    }
                                    else if (dataFieldPropertyInfo.PropertyType == typeof(decimal))
                                    {
                                        dataFieldPropertyInfo.SetValue(childObj, datarow[dtField.DataFieldName].ReturnZeroIfNull(), null);
                                    }
                                    else if (dataFieldPropertyInfo.PropertyType == typeof(String))
                                    {
                                        if (datarow[dtField.DataFieldName].GetType() == typeof(DateTime))
                                        {
                                            dataFieldPropertyInfo.SetValue(childObj, ConvertToDateString(datarow[dtField.DataFieldName]), null);
                                        }
                                        else
                                        {
                                            dataFieldPropertyInfo.SetValue(childObj, datarow[dtField.DataFieldName].ReturnEmptyIfNull(), null);
                                        }
                                    }

                                }
                            }

                            //Set the values for the children object
                            foreach (PropertyInfo masterPropertyInfo in childPropertInfoFields)
                            {
                                if (masterPropertyInfo.PropertyType.Name == childObj.GetType().Name)
                                {
                                    masterPropertyInfo.SetValue(masterObj, childObj);
                                }
                            }

                        }// end foreach
                    }
                    //Fill master Object with its related properties values
                    foreach (var dtField in masterObjectFields)
                    {

                        if (dtField != null)
                        {
                            // Get the property info object of this field, for easier accessibility
                            PropertyInfo dataFieldPropertyInfo = dtField.Property;

                            if (dataFieldPropertyInfo.PropertyType == typeof(DateTime))
                            {
                                dataFieldPropertyInfo.SetValue(masterObj, datarow[dtField.DataFieldName].ReturnDateTimeMinIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(int))
                            {
                                dataFieldPropertyInfo.SetValue(masterObj, datarow[dtField.DataFieldName].ReturnZeroIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(long))
                            {
                                dataFieldPropertyInfo.SetValue(masterObj, datarow[dtField.DataFieldName].ReturnZeroIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(decimal))
                            {
                                dataFieldPropertyInfo.SetValue(masterObj, datarow[dtField.DataFieldName].ReturnZeroIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(String))
                            {
                                if (datarow[dtField.DataFieldName].GetType() == typeof(DateTime))
                                {
                                    dataFieldPropertyInfo.SetValue(masterObj, ConvertToDateString(datarow[dtField.DataFieldName]), null);
                                }
                                else
                                {
                                    dataFieldPropertyInfo.SetValue(masterObj, datarow[dtField.DataFieldName].ReturnEmptyIfNull(), null);
                                }
                            }
                        }//end if
                    }//end foreach

                    lock (dataList)
                    {
                        dataList.Add(masterObj);
                    }
                });

            return dataList;
        }

        /// <summary>
        /// Converts datatable to list<T> dynamically
        /// </summary>
        /// <typeparam name="T">Class name</typeparam>
        /// <param name="dataTable">data table to convert</param>
        /// <returns>List<T></returns>
        public static List<T> ConvertToList_OLD<T>(this DataTable dataTable) where T : class, new()
        {
            var dataList = new List<T>();
            
            // List of class property infos
            List<PropertyInfo> propertyInfoFields = new List<PropertyInfo>();

            //List of T object data fields (DbColumnAttribute Values), and types.
            List<ObjectPropertyInfoField> objectFields = new List<ObjectPropertyInfoField>();

            //Define what attributes to be read from the class
            const BindingFlags flags = BindingFlags.Public | BindingFlags.Instance;

            // Initialize the property info fields list
            propertyInfoFields = typeof(T).GetProperties(flags)
                .Where(property => property.GetCustomAttribute<DbColumnAttribute>() != null)
                .Cast<PropertyInfo>()
                .ToList();
            
            // Initialize the object data fields  list
            foreach(var item in propertyInfoFields)
            {
                objectFields.Add(new ObjectPropertyInfoField {
                    Property = item,
                    DataFieldName = item.GetCustomAttribute<DbColumnAttribute>().Name,
                    DataFieldType = Nullable.GetUnderlyingType(item.PropertyType) ?? item.PropertyType
                });
            }

            //Read Datatable column names and types
            var dtlFieldNames = dataTable.Columns.Cast<DataColumn>()
                .Select(item => new { 
                    Name = item.ColumnName, 
                    Type = item.DataType 
                }).ToList();

            //Begin data table processing
            Parallel.ForEach(
                dataTable.AsEnumerable().ToList(),
                (datarow) =>
                {
                    var classObj = new T();

                    foreach(var dtField in dtlFieldNames)
                    {
                        var dataField = objectFields.Find(item => item.DataFieldName == dtField.Name);

                        if (dataField != null)
                        {
                            // Get the property info object of this field, for easier accessibility
                            PropertyInfo dataFieldPropertyInfo = dataField.Property;

                            if (dataFieldPropertyInfo.PropertyType == typeof(DateTime))
                            {
                                dataFieldPropertyInfo.SetValue(classObj, datarow[dtField.Name].ReturnDateTimeMinIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(int))
                            {
                                dataFieldPropertyInfo.SetValue(classObj, datarow[dtField.Name].ReturnZeroIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(long))
                            {
                                dataFieldPropertyInfo.SetValue(classObj, datarow[dtField.Name].ReturnZeroIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(decimal))
                            {
                                dataFieldPropertyInfo.SetValue(classObj, datarow[dtField.Name].ReturnZeroIfNull(), null);
                            }
                            else if (dataFieldPropertyInfo.PropertyType == typeof(String))
                            {
                                if (datarow[dtField.Name].GetType() == typeof(DateTime))
                                {
                                    dataFieldPropertyInfo.SetValue(classObj, ConvertToDateString(datarow[dtField.Name]), null);
                                }
                                else
                                {
                                    dataFieldPropertyInfo.SetValue(classObj, datarow[dtField.Name].ReturnEmptyIfNull(), null);
                                }
                            }
                        }
                    }

                    lock (dataList)
                    {
                        dataList.Add(classObj);
                    }
                });

            return dataList;
        }

        private static string ConvertToDateString(object date) 
        {
            if (date == null)
                return string.Empty;
           
            return Convert.ToDateTime(date).ConvertDate();
        }

    }
}
