using CMS.Base;
using CMS.CMSImportExport;
using CMS.Core;
using CMS.DataEngine;
using CMS.Helpers;
using CMS.IO;
using CMS.Membership;
using CMS.Modules;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Xml;
using FileStream = CMS.IO.FileStream;

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
            string packagePath = args[0] ?? "";
            string userName = args[1] ?? "";
            string applicationPath = args[2] ?? "";
            bool moduleExport = false;
            string moduleName = "";

            if (string.IsNullOrEmpty(packagePath))
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

            ImportModule(packagePath, userName);

            ImportObject(packagePath, userName);

            string objectType = "";
            int objectID = 0;

            SiteExportSettings siteExportSettings = new SiteExportSettings(UserInfoProvider.GetUserInfo(userName))
            {
                WebsitePath = SystemContext.WebApplicationPhysicalPath,
                TargetPath = packagePath + "Export",
                CreatePackage = true,
                TargetFileName = $"ExportPackage_{DateTime.Now.ToString()}",
                TemporaryFilesPath = "/path",
                

            };
            //switch on one object vs global object selection
            var selectedObjects = ObjectSelections(objectType);

            if (objectID > 0)
            {
                var singleObject = SingleObjectSelection(objectID, selectedObjects);
                siteExportSettings.Select(singleObject.TypeInfo.ObjectType,singleObject.TypeInfo.ObjectClassName,singleObject.TypeInfo.IsSiteObject);
            }

            if(moduleExport)
            {
                siteExportSettings.SetInfo(ImportExportHelper.MODULE_NAME, moduleName);
            }
            
            siteExportSettings.Select(selectedObjects.TypeInfo.ObjectType, selectedObjects.TypeInfo.ObjectClassName, selectedObjects.TypeInfo.IsSiteObject);

            //Preset for global object selection
            siteExportSettings.SelectGlobalObjects(new List<string>(), "");

            // Make sure no data is in temp folder (possibly from previous unsuccessful export)
            ExportProvider.DeleteTemporaryFiles(siteExportSettings, true);
            ExportManager exportManager = new ExportManager(siteExportSettings);
            exportManager.Export(null);

            //Cleanup
            ExportProvider.DeleteTemporaryFiles(siteExportSettings, true);

            return;
        }

        private static BaseInfo ObjectSelections(string objectType)
        {
            //Info Based on object type for type based filtering 
            BaseInfo infoObj = ModuleManager.GetReadOnlyObject(objectType); 

            return infoObj;

        }

        private static GeneralizedInfo SingleObjectSelection(int objectID, BaseInfo infoObj)
        {
            //object level filtering to lower levels
            GeneralizedInfo exportObj = infoObj.GetObject(objectID);
            return exportObj;

        }

        private static void ImportObject(string packagePath, string userName)
        {
            string objectType = "";
            int objectID = 0;

            SiteImportSettings settings = new SiteImportSettings(UserInfoProvider.GetUserInfo(userName));
            settings.UseAutomaticSiteForTranslation = true;
            settings.LogSynchronization = true;
            settings.WebsitePath = SystemContext.WebApplicationPhysicalPath;
            settings.SourceFilePath = packagePath;

            var obj = ObjectSelections(objectType);

            var importObj = SingleObjectSelection(objectID, obj);

            if (importObj.ObjectSiteID>0)
            {
                settings.SiteId = importObj.ObjectSiteID;
                settings.ExistingSite = true;
                settings.SiteIsContentOnly = settings.SiteInfo.SiteIsContentOnly;

                // Do not update site definition when restoring a single object
                settings.SetSettings(ImportExportHelper.SETTINGS_UPDATE_SITE_DEFINITION, false);
            }

            ImportProvider.CreateTemporaryFiles(settings);

            settings.LoadDefaultSelection();

            ImportProvider.ImportObjectsData(settings);
        }

        private static DataSet GetEmptyDataSet(SiteImportSettings settings, string objectType, bool siteObjects, bool selectionOnly, out GeneralizedInfo infoObj, bool forceXMLStructure = false)
        {
            DataSet ds;

            // Raise prepare data event
            var eData = new ImportGetDataEventArgs
            {
                Settings = settings,
                ObjectType = objectType,
                SiteObjects = siteObjects,
                SelectionOnly = selectionOnly
            };

            // Handle the event
            SpecialActionsEvents.GetEmptyObject.StartEvent(eData);

            // Ensure empty object
            infoObj = eData.Object ?? ModuleManager.GetReadOnlyObject(objectType);

            // Get data set
            if (forceXMLStructure || (infoObj == null) || (infoObj.MainObject is NotImplementedInfo))
            {
                // Create empty data set
                ds = new DataSet();

                // Ensure translation table
                if (objectType == ImportExportHelper.OBJECT_TYPE_TRANSLATION)
                {
                    ds.Tables.Add(TranslationHelper.GetEmptyTable());
                }
            }
            else
            {
                // Get objects DataSet
                if (selectionOnly)
                {
                    // Code name column
                    ds = DataHelper.GetSingleColumnDataSet(ObjectHelper.GetSerializationTableName(infoObj), infoObj.CodeNameColumn, typeof(string));

                    // Display name column
                    var dt = ds.Tables[0];
                    if (infoObj.CodeNameColumn != infoObj.DisplayNameColumn)
                    {
                        DataHelper.EnsureColumn(dt, infoObj.DisplayNameColumn, typeof(string));
                    }

                    // GUID column
                    var ti = infoObj.TypeInfo;
                    if ((ti.GUIDColumn != ObjectTypeInfo.COLUMN_NAME_UNKNOWN) && (infoObj.CodeNameColumn != ti.GUIDColumn))
                    {
                        DataHelper.EnsureColumn(dt, ti.GUIDColumn, typeof(Guid));
                    }

                    // Columns used by type condition
                    var tc = ti.TypeCondition;
                    if (tc != null)
                    {
                        foreach (var conditionColumn in tc.ConditionColumns)
                        {
                            DataHelper.EnsureColumn(dt, conditionColumn, ti.ClassStructureInfo.GetColumnType(conditionColumn));
                        }
                    }
                }
                else
                {
                    ds = ObjectHelper.GetObjectsDataSet(OperationTypeEnum.Export, infoObj, true);
                }

                // Add tasks table
                DataSet tasksDS;
                if (selectionOnly)
                {
                    tasksDS = DataHelper.GetSingleColumnDataSet("Export_Task", "TaskID", typeof(int));

                    DataTable dt = tasksDS.Tables[0];
                    dt.Columns.Add("TaskTitle", typeof(string));
                    dt.Columns.Add("TaskType", typeof(string));
                    dt.Columns.Add("TaskTime", typeof(DateTime));
                }
                else
                {
                    tasksDS = DataClassInfoProvider.GetDataSet("Export.Task");
                }

                DataHelper.TransferTable(ds, tasksDS, "Export_Task");
            }

            return ds;
        }

        public static DataSet LoadObjects(SiteImportSettings settings, string objectType, bool siteObjects, bool selectionOnly = false, bool forceXMLStructure = false)
        {
            DataSet ds;

            var e = new ImportGetDataEventArgs
            {
                Settings = settings,
                ObjectType = objectType,
                SiteObjects = siteObjects,
                SelectionOnly = selectionOnly
            };

                // Get empty data set
            GeneralizedInfo infoObj;
            ds = GetEmptyDataSet(settings, objectType, siteObjects, selectionOnly, out infoObj, forceXMLStructure);

            // Turn off constrains check
            ds.EnforceConstraints = false;

            // Raise prepare data event
            var eData = new ImportGetDataEventArgs
            {
                Data = ds,
                Settings = settings,
                ObjectType = objectType,
                SiteObjects = siteObjects,
                SelectionOnly = selectionOnly
            };

            // Handle the event
            SpecialActionsEvents.PrepareDataStructure.StartEvent(eData);

            // Lowercase table names for compatibility
            DataHelper.LowerCaseTableNames(ds);


            Stream reader = null;
            XmlReader xml = null;

                string safeObjectType = ImportExportHelper.GetSafeObjectTypeName(objectType);

                // Prepare the path
                string filePath = DirectoryHelper.CombinePath(settings.TemporaryFilesPath, ImportExportHelper.DATA_FOLDER) + "\\";

                // Get object type subfolder
                filePath += ImportExportHelper.GetObjectTypeSubFolder(settings, objectType, siteObjects);
                filePath += safeObjectType + ".xml";
                string rootElement = safeObjectType;

                filePath = ImportExportHelper.GetExportFilePath(filePath);

                // Import only if file exists
                if (System.IO.File.Exists(filePath))
                {
                    // Reader setting
                    XmlReaderSettings rs = new XmlReaderSettings();
                    rs.CloseInput = true;
                    rs.CheckCharacters = false;

                    // Open reader
                    reader = FileStream.New(filePath, CMS.IO.FileMode.Open, CMS.IO.FileAccess.Read, CMS.IO.FileShare.Read, 8192);
                    xml = XmlReader.Create(reader, rs);

                    // Read main document element
                    do
                    {
                        xml.Read();
                    } while ((xml.NodeType != XmlNodeType.Element) && !xml.EOF);

                    if (xml.Name.ToLowerInvariant() != rootElement.ToLowerInvariant())
                    {
                        throw new Exception("[ImportProvider.LoadObjects]: The required page element is '" + safeObjectType + "', element found is '" + xml.Name + "'.");
                    }

                    // Get version
                    if (xml.HasAttributes)
                    {
                        xml.MoveToAttribute("version");
                    }

                    if ((xml.NodeType != XmlNodeType.Attribute) || (xml.Name.ToLowerInvariant() != "version"))
                    {
                        throw new Exception("[ImportProvider.LoadObjects]: Cannot find version attribute.");
                    }


                    // Get the dataset
                    do
                    {
                        xml.Read();
                    } while (((xml.NodeType != XmlNodeType.Element) || (xml.Name != "NewDataSet")) && !xml.EOF);

                    // Read data
                    if (xml.Name == "NewDataSet")
                    {
                        ds.ReadXml(xml);
                    }

                    // Filter data by object type condition for selection, some object types may overlap
                    if (selectionOnly && !DataHelper.DataSourceIsEmpty(ds))
                    {
                        // Remove unwanted rows from data collection with dependence on type condition
                        if (infoObj.TypeInfo.TypeCondition != null)
                        {
                            var dt = ObjectHelper.GetTable(ds, infoObj);
                            var where = infoObj.TypeInfo.WhereCondition;
                            where.Replace(" N'", " '").Replace("(N'", "('");
                            DataHelper.KeepOnlyRows(dt, where);

                            dt.AcceptChanges();
                        }
                    }
                }


                // Safely close readers
                if (xml != null)
                {
                    xml.Close();
                }

                if (reader != null)
                {
                    reader.Close();
                }

            return ds;
        }
        

        private static void ImportModule(string modulePath, string userName)
        {
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
