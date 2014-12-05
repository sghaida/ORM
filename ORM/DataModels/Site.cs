﻿using ORM.Libs;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ORM.DataAccess;
using ORM.DataAttributes;

namespace ORM.DataModels
{
    [DataSource(Name = "Sites", Type = GLOBALS.DataSource.Type.DBTable, AccessMethod = GLOBALS.DataSource.AccessMethod.SingleSource)]
    public class Site : DataModel
    {
        [IsIDField]
        [DbColumn("SiteID")]
        public int ID { get; set; }

        [DbColumn("SiteName")]
        public string Name { get; set; }

        [DbColumn("CountryCode")]
        public string CountryCode { get; set; }

        [DbColumn("Description")]
        public string Description { get; set; }

        //[DbColumn("CountryId")]
        //public string CountryId { get; set; }

        //[DataRelation(WithDataModel = typeof(Country), OnDataModelKey = "ID", ThisKey = "CountryId")]
        //public Country SiteCountry { get; set; }
    }
}