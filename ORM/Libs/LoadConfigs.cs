using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ORM.Libs
{
    public class LoadConfigs
    {
        public Configuration DllConfig { get; private set; }

        public LoadConfigs() 
        {
           DllConfig = ConfigurationManager.OpenExeConfiguration(this.GetType().Assembly.Location);
        }

        public AppSettingsSection LoadConfigsSection(string sectionName)
        {
            return (AppSettingsSection)DllConfig.GetSection(sectionName);
        }

        public string GetConfigValue(string sectionName, string key) 
        {
            return LoadConfigsSection(sectionName).Settings[key].Value; 
        }

        
    }
}
