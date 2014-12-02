using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;

namespace ORM.Helpers
{
    public static class HelperFunctions
    {
        public static bool GetResolvedConnecionIPAddress(string serverNameOrURL, out string resolvedIPAddress)
        {
            bool isResolved = false;
            IPHostEntry hostEntry = null;
            IPAddress resolvIP = null;
            try
            {
                if (!IPAddress.TryParse(serverNameOrURL, out resolvIP))
                {
                    hostEntry = Dns.GetHostEntry(serverNameOrURL);

                    if (hostEntry != null && hostEntry.AddressList != null
                                 && hostEntry.AddressList.Length > 0)
                    {
                        if (hostEntry.AddressList.Length == 1)
                        {
                            resolvIP = hostEntry.AddressList[0];
                            isResolved = true;
                        }
                        else
                        {
                            foreach (IPAddress var in hostEntry.AddressList)
                            {
                                if (var.AddressFamily == AddressFamily.InterNetwork)
                                {
                                    resolvIP = var;
                                    isResolved = true;
                                    break;
                                }
                            }
                        }
                    }
                }
                else
                {
                    isResolved = true;
                }
            }
            catch (Exception ex)
            {
                isResolved = false;
                resolvIP = null;
            }
            finally
            {
                resolvedIPAddress = resolvIP.ToString();
            }

            return isResolved;
        }


        public static string SerializeObject<T>(T source)
        {
            var serializer = new XmlSerializer(typeof(T));

            using (var sw = new System.IO.StringWriter())
            using (var writer = new XmlTextWriter(sw))
            {
                serializer.Serialize(writer, source);
                return sw.ToString();
            }
        }

        public static T DeSerializeObject<T>(string xml)
        {
            using (System.IO.StringReader sr = new System.IO.StringReader(xml))
            {
                XmlSerializer serializer = new XmlSerializer(typeof(T));
                return (T)serializer.Deserialize(sr);
            }
        }

        public static object ReturnZeroIfNull(this object value)
        {
            if (value == System.DBNull.Value)
                return 0;
            else if (value == null)
                return 0;
            else
                return value;
        }

        public static object ReturnEmptyIfNull(this object value)
        {
            if (value == System.DBNull.Value)
                return string.Empty;
            else if (value == null)
                return string.Empty;
            else
                return value;
        }

        public static object ReturnFalseIfNull(this object value)
        {
            if (value == System.DBNull.Value)
                return false;
            else if (value == null)
                return false;
            else
                return value;
        }

        public static object ReturnDateTimeMinIfNull(this object value)
        {
            if (value == System.DBNull.Value)
                return DateTime.MinValue;
            else if (value == null)
                return DateTime.MinValue;
            else
                return value;
        }

        public static object ReturnNullIfDBNull(this object value)
        {
            if (value == System.DBNull.Value)
                return '\0';
            else if (value == null)
                return '\0';
            else
                return value;
        }

        //This function formats the display-name of a user,
        //and removes unnecessary extra information.
        public static string FormatUserDisplayName(string displayName = null, string defaultValue = "tBill Users", bool returnNameIfExists = false, bool returnAddressPartIfExists = false)
        {
            //Get the first part of the Users's Display Name if s/he has a name like this: "firstname lastname (extra text)"
            //removes the "(extra text)" part
            if (!string.IsNullOrEmpty(displayName))
            {
                if (returnNameIfExists == true)
                    return Regex.Replace(displayName, @"\ \(\w{1,}\)", "");
                else
                    return (displayName.Split(' '))[0];
            }
            else
            {
                if (returnAddressPartIfExists == true)
                {
                    var emailParts = defaultValue.Split('@');
                    return emailParts[0];
                }
                else
                    return defaultValue;
            }
        }

        public static string FormatUserTelephoneNumber(this string telephoneNumber)
        {
            string result = string.Empty;

            if (!string.IsNullOrEmpty(telephoneNumber))
            {
                //result = telephoneNumber.ToLower().Trim().Trim('+').Replace("tel:", "");
                result = telephoneNumber.ToLower().Trim().Replace("tel:", "");

                if (result.Contains(";"))
                {
                    if (!result.ToLower().Contains(";ext="))
                        result = result.Split(';')[0].ToString();
                }
            }

            return result;
        }

        public static bool IsValidEmail(this string emailAddress)
        {
            string pattern = @"\A(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)\Z";

            return Regex.IsMatch(emailAddress, pattern);
        }

        public static string ConvertDate(this DateTime datetTime, bool excludeHoursAndMinutes = false)
        {
            if (datetTime != DateTime.MinValue || datetTime != null)
            {
                if (excludeHoursAndMinutes == true)
                    return datetTime.ToString("yyyy-MM-dd");
                else
                    return datetTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
            }
            else
                return null;
        }

        public static string ConvertSecondsToReadable(this int secondsParam)
        {
            int hours = Convert.ToInt32(Math.Floor((double)(secondsParam / 3600)));
            int minutes = Convert.ToInt32(Math.Floor((double)(secondsParam - (hours * 3600)) / 60));
            int seconds = secondsParam - (hours * 3600) - (minutes * 60);

            string hours_str = hours.ToString();
            string mins_str = minutes.ToString();
            string secs_str = seconds.ToString();

            if (hours < 10)
            {
                hours_str = "0" + hours_str;
            }

            if (minutes < 10)
            {
                mins_str = "0" + mins_str;
            }
            if (seconds < 10)
            {
                secs_str = "0" + secs_str;
            }

            return hours_str + ':' + mins_str + ':' + secs_str;
        }


        public static string ConvertSecondsToReadable(this long secondsParam)
        {
            int hours = Convert.ToInt32(Math.Floor((double)(secondsParam / 3600)));
            int minutes = Convert.ToInt32(Math.Floor((double)(secondsParam - (hours * 3600)) / 60));
            int seconds = Convert.ToInt32(secondsParam - (hours * 3600) - (minutes * 60));

            string hours_str = hours.ToString();
            string mins_str = minutes.ToString();
            string secs_str = seconds.ToString();

            if (hours < 10)
            {
                hours_str = "0" + hours_str;
            }

            if (minutes < 10)
            {
                mins_str = "0" + mins_str;
            }
            if (seconds < 10)
            {
                secs_str = "0" + secs_str;
            }

            return hours_str + ':' + mins_str + ':' + secs_str;
        }
    }
}
