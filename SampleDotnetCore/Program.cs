using System;
using System.IO;

using xresloader;
namespace SampleDotnetCore
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            ExcelConfigManager.PackageName = "";
            ExcelConfigManager.FileGetFullPath = (p) => { return String.Format("../SampleData/{0}", p); };
            // ExcelConfigManager.FileReadStream = (path) => { return File.OpenRead(path); };

            ExcelConfigManager.Init(new string[] { "pb_header.pb", "kind.pb" });

            // 添加配置
            ExcelConfigSet set = ExcelConfigManager.AddConfig("arr_in_arr_cfg", null, "arr_in_arr_cfg.bin");
            if (null == set) { 
                Console.WriteLine(ExcelConfigManager.LastError);
                return;
            }

            // 添加索引Key-Value
            set.AddKVIndexAuto("id");

            // 加载全部的配置
            ExcelConfigManager.ReloadAll();

            // 取数据
            var item = set.GetKVAuto(10001U);
            if (null == item)
            {
                Console.WriteLine("arr_in_arr_cfg 10001 not found");
            }
            else
            {
                Console.WriteLine(item.ToString());
                var arr = item.GetFieldList("arr");
                foreach (var msg in arr) {
                    Console.WriteLine(String.Format("Name={0}", ((xresloader.Protobuf.DynamicMessage)msg).GetFieldValue("name")));
                }
            }
        }
    }
}
