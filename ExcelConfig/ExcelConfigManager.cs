using System.IO;
using ProtoBuf;
using System.Collections.Generic;

using xresloader.Protobuf;

namespace xresloader {
    public static class ExcelConfigManager {
        const string CLASS_NAME = "ExcelConfigManager";
        public delegate Stream FileReadStreamHandle(string file_path);
        public delegate string FileGetFullPathHandle(string file_path);

        static private DynamicFactory factory = new DynamicFactory();
        static private Dictionary<string, ExcelConfigSet> allConfigures = new Dictionary<string, ExcelConfigSet>();

        static private FileReadStreamHandle readFileStream = null;
        static private FileGetFullPathHandle getFileFullPath = null;

        static private string packageName = "";
        static private string lastError = "";

        static public DynamicFactory Factory {
            get { return factory; }
        }

        static public FileReadStreamHandle FileReadStream
        {
            get { return readFileStream; }
            set { readFileStream = value; }
        }

        static public FileGetFullPathHandle FileGetFullPath
        {
            get { return getFileFullPath; }
            set { getFileFullPath = value; }
        }

        static public string PackageName
        {
            get { return packageName; }
            set { packageName = value; }
        }

        static public string LastError
        {
            get { return lastError; }
            set { lastError = value; }
        }

        static public Stream GetFileStream(string path) {
            if (null != getFileFullPath) {
                path = getFileFullPath(path);
            }

            if (null != readFileStream) {
                return readFileStream(path);
            }

            return File.OpenRead(path);
        }

        // Use this for initialization
        static public void Init(string[] pb_files) {
            for (int i = 0; i < pb_files.Length; ++i) {
                if (null != pb_files[i]) {
                    factory.Register(GetFileStream(pb_files[i]));
                }
            }
        }

        /// <summary>
        /// 添加配置
        /// </summary>
        /// <param name="config_name">配置名称</param>
        /// <param name="protocol_name">protobuf协议名称</param>
        /// <param name="file_name">文件名</param>
        /// <returns></returns>
        static public ExcelConfigSet AddConfig(string config_name, string protocol_name = "", string file_name = "") {
            if (null == protocol_name || protocol_name.Length == 0) {
                if (null != packageName && packageName.Length > 0)
                {
                    protocol_name = string.Format("{0}.{1}", packageName, config_name);
                }
                else
                {
                    protocol_name = config_name;
                }
            }

            if (null != Get(config_name)) {
                lastError = string.Format("configure name {0} already registered, can not register again", config_name);
                return new ExcelConfigSet(factory, file_name, protocol_name);
            }

            ExcelConfigSet ret = new ExcelConfigSet(factory, file_name, protocol_name);
            allConfigures[config_name] = ret;
            return ret;
        }

        /// <summary>
        /// 重新加载全部配置
        /// </summary>
        static public void ReloadAll() {
            foreach (var cfg in allConfigures) {
                cfg.Value.Reload();
            }
        }

        /// <summary>
        /// 获取某个配置集
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        static public ExcelConfigSet Get(string name) {
            ExcelConfigSet ret;
            if (allConfigures.TryGetValue(name, out ret)) {
                return ret;
            }

            return null;
        }

        static private void RegisterAllConfigure() {
            //Check TableConfigure
             
            //AddConfig("const_parameter_cfg")
            //    .AddKVIndex((DynamicMessage item) =>{
            //    return new ExcelConfigSet.Key(item.GetFieldValue("id"));
            //    }).AddFilter((DynamicMessage item) => {
            //    return (uint)item.GetFieldValue("id") > 0;
            //});

            //AddConfig("const_parameter_cfg").AddKVIndexAuto("id").AddFilter(item => (uint)item.GetFieldValue("id") > 0);
            //DynamicMessage msg = Get("errorcode_cfg").GetKVAuto(0);
            //msg.GetFieldValue("level");


            //AddConfig("errorcode_cfg").AddKVIndex((DynamicMessage item) => {
                 
            //    return new ExcelConfigSet.Key(item.GetFieldValue("id"));
            //});

            // ============= test code here =============
            //ReloadAll();
            //ExcelConfigSet errcode_set = Get("errorcode_cfg");
            //_Log.Debug("error code [{0}] = {1}", 0, errcode_set.GetKV(new ExcelConfigSet.Key(0)).GetFieldValue("text"));
            //_Log.Debug("error code [{0}] = {1}", 1, errcode_set.GetKV(new ExcelConfigSet.Key(1)).GetFieldValue("text"));

            //Get("errorcode_cfg").GetKV(new ExcelConfigSet.Key(0));
        }
    }

}