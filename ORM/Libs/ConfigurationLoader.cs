using System.Configuration;

namespace ORM.Libs
{
    public class ConfigurationLoader
    {
        public Configuration DllConfig { get; private set; }

        public ConfigurationLoader()
        {
            string location = GetType().Assembly.Location;
            string location2 = GetType().BaseType.Assembly.Location;
            DllConfig = ConfigurationManager.OpenExeConfiguration(location2);
        }

        public AppSettingsSection LoadConfigsSection(string sectionName)
        {
            return ((AppSettingsSection)DllConfig.GetSection(sectionName));
        }

        public string GetConfigValue(string sectionName, string key)
        {
            return LoadConfigsSection(sectionName).Settings[key].Value;
        }

    }

}