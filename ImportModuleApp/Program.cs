using CMS.Base;
using CMS.CMSImportExport;
using CMS.Core;
using CMS.DataEngine;
using CMS.IO;
using CMS.Membership;
using CMS.Modules;
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Xml;

namespace ImportModuleApp
{  
    class Program
    {
        public const string INSTALLED = "installed";
        public const string INSTALLED_PENDING_RESTART = "installed_pending_restart";
        public const string SQL_BEFORE_PATH = "";
        public const string SQL_AFTER_PATH = "";

        static void Main(string[] args)
        {
            string modulePath = args[0] ?? "";
            string userName = args[1] ?? "";
            string applicationPath = args[2] ?? "";

            if (string.IsNullOrEmpty(modulePath))
            {
                Console.Error.WriteLine("No package path provided");
                return;
            }

            if (string.IsNullOrEmpty(applicationPath))
            {
                Console.Error.WriteLine("No main application path provided");
                return;
            }

            //Initialize Kentico API for external application
            CMSApplication.Init();

            SystemContext.WebApplicationPhysicalPath = applicationPath;

            try
            {
                string moduleName = GetModuleName(modulePath);

                using (var transaction = new CMSTransactionScope())
                {
                    // Execute before.sql
                    ExecuteSqlScript(SQL_BEFORE_PATH);

                    // Import .zip package with export data
                    if (!System.IO.File.Exists(modulePath))
                    {
                        throw new ArgumentException("[ModuleExportPackageImporter.Import] File on given path '" + modulePath + "' does not exist.", "modulePath");
                    }

                    var settings = CreateImportSettings(modulePath, userName);

                    ImportManager importManager = new ImportManager(settings);
                    importManager.Import(null);

                    // Execute after.sql
                    ExecuteSqlScript(SQL_AFTER_PATH);

                    // Commit the module installation if the transaction succeeded
                    transaction.Commit();
                }

                // Perform finish actions
                FinishModuleInstallation(modulePath);



                CoreServices.EventLog.LogEvent("I", "ModuleInstaller", "MODULEINSTALLED", $"Module '{moduleName}' from path {modulePath} has been installed (restart could be required).");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
            }

        }

        private static string GetModuleName(string modulePath)
        {
            string resourceName = "";

            using (ZipArchive archive = ZipFile.OpenRead(modulePath))
            {
                foreach (ZipArchiveEntry entry in archive.Entries)
                {
                    if (entry.FullName.Contains("cms_resource.xml.export"))
                    {
                        var XMLDocument = new XmlDocument();
                        XMLDocument.Load(entry.Open());
                        resourceName = XMLDocument.SelectSingleNode("ResourceName").Value;
                    }
                }
            }
            if (!string.IsNullOrEmpty(resourceName))
                return resourceName;
            throw new Exception($"Module name does not exist in ZipFile on path {modulePath}");
        }

        private static void FinishModuleInstallation(string resourceName)
        {
            if (!string.IsNullOrEmpty(resourceName))
            {
                ResourceInfo resource = ResourceInfoProvider.GetResources().WhereEquals(nameof(Module.ModuleInfo.Name), resourceName).FirstOrDefault();
                resource.ResourceInstallationState = INSTALLED;
                //Potential to set other properties
                resource.ResourceVersion = "1.0";
                resource.ResourceIsInDevelopment = true;
                ResourceInfoProvider.SetResourceInfo(resource);
            }
        }
                 

        public static void ExecuteSqlScript(string installationBeforeSqlPath)
        {
            if (!System.IO.File.Exists(installationBeforeSqlPath))
            {
                return;
            }

            // Read the script with default encoding
            string sqlQuery = System.IO.File.ReadAllText(installationBeforeSqlPath);
            ConnectionHelper.ExecuteNonQuery(sqlQuery, null, QueryTypeEnum.SQLQuery);
        }

        private static SiteImportSettings CreateImportSettings(string sourceFilePath, string userName)
        {
            SiteImportSettings result = new SiteImportSettings(UserInfoProvider.GetUserInfo(userName));
            result.SourceFilePath = sourceFilePath;
            result.WebsitePath = SystemContext.WebApplicationPhysicalPath;

            result.ImportType = ImportTypeEnum.AllForced;
            result.CopyFiles = false;
            result.CopyCodeFiles = false;
            result.DefaultProcessObjectType = ProcessObjectEnum.All;
            result.LogSynchronization = false;
            result.RefreshMacroSecurity = true;

            return result;
        }
       
    }
}
