using System.Collections;
using ProtoBuf;
using System.Collections.Generic;
using System;
using System.IO;

using xresloader.Protobuf;

namespace xresloader {
    public class ExcelConfigSet {
        private string fileName;
        private string protocol;
        private DynamicFactory factory;
        private List<DynamicMessage> datas = new List<DynamicMessage>();

        public List<DynamicMessage> Datas { get { return datas; } }

        public struct Key {
            public object[] keys;

            public int Size { get { return keys == null ? 0 : keys.Length; } }
            public Key(params object[] collection) {
                keys = collection;
            }

            public Key(object data) {
                keys = new object[1] { data };
            } 

            public override int GetHashCode() {
                int ret = 0;
                for (int i = 0; i < keys.Length; ++i) {
                    ret ^= keys[i].GetHashCode();
                }

                return ret;
            }

            public bool Equals(Key obj) {
                if (Size != obj.Size) {
                    return false;
                }

                for (int i = 0; i < keys.Length; ++i) {
                    if (!keys[i].Equals(obj.keys[i])) {
                        return false;
                    }
                }

                return true;
            }

            public override bool Equals(object obj) {
                if (!(obj is Key)) {
                    return false;
                }

                return Equals((Key)obj);
            }
        }

        public delegate Key IndexFunction(DynamicMessage msg);
        public delegate bool FilterFunction(DynamicMessage msg);


        private class KVIndexData {
            public IndexFunction Handle;
            public Dictionary<Key, DynamicMessage> Index;
        }

        private class KLIndexData {
            public IndexFunction Handle;
            public Dictionary<Key, List<DynamicMessage>> Index;
            public IComparer<DynamicMessage> SortRule;
        }

        private List<FilterFunction> filters = new List<FilterFunction>();
        private List<KVIndexData> kvIndex = new List<KVIndexData>();
        private List<KLIndexData> klIndex = new List<KLIndexData>();

        public ExcelConfigSet(DynamicFactory fact, string file_name, string protocol_name) {
            factory = fact;
            fileName = file_name;
            protocol = protocol_name;
        }

        public string FileName {
            get { return fileName; }
        }

        public string Protocol {
            get { return protocol; }
        }

        /// <summary>
        /// 清空配置
        /// </summary>
        public void Clear() {
            datas.Clear();

            foreach (var index in kvIndex) {
                index.Index.Clear();
            }

            foreach (var index in klIndex) {
                index.Index.Clear();
            }
        }

        /// <summary>
        /// 重新加载配置
        /// </summary>
        /// <returns>返回是否加载成功</returns>
        public bool Reload() {
            Clear();

            try {
                var header_desc = factory.GetMsgDiscriptor("com.owent.xresloader.pb.xresloader_datablocks");
                if (null == header_desc) {
                    ExcelConfigManager.LastError = string.Format("load configure file {0} failed, com.owent.xresloader.pb.xresloader_datablocks not registered", fileName);
                    return false;
                }

                var msg_desc = factory.GetMsgDiscriptor(protocol);
                if (null == msg_desc) {
                    ExcelConfigManager.LastError = string.Format("load configure file {0} failed, {1} not registered", fileName, protocol);
                    return false;
                }

                DynamicMessage data_set = factory.Decode(header_desc, ExcelConfigManager.GetFileStream(fileName));
                if (null == data_set) {
                    ExcelConfigManager.LastError = string.Format("load configure file {0} failed, {1}", fileName, factory.LastError);
                    return false;
                }

                foreach(var cfg_item in data_set.GetFieldList("data_block")) {
                    DynamicMessage data_item = factory.Decode(msg_desc, new MemoryStream((byte[])cfg_item));
                    if (null == data_item) {
                        ExcelConfigManager.LastError = string.Format("load configure file {0} failed, {1}", fileName, factory.LastError);
                        continue;
                    }

                    bool filter_pass = true;
                    foreach (var fn in filters) {
                        filter_pass = fn(data_item);
                        if (!filter_pass) {
                            break;
                        }
                    }

                    if (!filter_pass) {
                        continue;
                    }

                    datas.Add(data_item);
                    foreach (var index in kvIndex) {
                        if (null != index.Handle) {
                            Key key = index.Handle(data_item);
                            index.Index[key] = data_item;
                        }
                    }

                    foreach (var index in klIndex) {
                        if (null != index.Handle) {
                            List<DynamicMessage> ls;
                            Key key = index.Handle(data_item);
                            if (index.Index.TryGetValue(key, out ls)) {
                                ls.Add(data_item);
                            } else {
                                index.Index[key] = new List<DynamicMessage> { data_item };
                            }
                        }
                    }
                }

                foreach (var index in klIndex) {
                    if (null != index.SortRule) {
                        foreach (var ls in index.Index) {
                            ls.Value.Sort(index.SortRule);
                        }
                    }
                }

            } catch (Exception e) {
                ExcelConfigManager.LastError = string.Format("{0}", e.Message);
                return false;
            }

            return true;
        }

        /// <summary>
        /// 添加Key-Value型索引
        /// </summary>
        /// <param name="index">索引ID，从0开始，每一组配置都可以添加多个索引。请保持索引ID尽量小</param>
        /// <param name="fn">取索引Key的函数</param>
        /// <returns>自身</returns>
        public ExcelConfigSet AddKVIndex(int index, IndexFunction fn) {
            if (index < 0) {
                throw new ArgumentException("index must not be negetive");
            }

            if (null == fn) {
                throw new ArgumentNullException("IndexFunction");
            }

            while (kvIndex.Count <= index) {
                KVIndexData obj = new KVIndexData();
                obj.Handle = null;
                obj.Index = new Dictionary<Key, DynamicMessage>();
                kvIndex.Add(obj);
            }

            KVIndexData index_set = kvIndex[index];
            index_set.Handle = fn;

            foreach (var data_item in datas) {
                index_set.Index[index_set.Handle(data_item)] = data_item;
            }
            return this;
        }

        /// <summary>
        /// 添加Key-Value型索引，索引ID为0
        /// </summary>
        /// <param name="fn">取索引Key的函数</param>
        /// <returns>自身</returns>
        public ExcelConfigSet AddKVIndex(IndexFunction fn) { return AddKVIndex(0, fn); }

        /// <summary>
        /// 按协议的字段名添加Key-Value型索引，索引ID为0
        /// </summary>
        /// <param name="key">字段名</param>
        /// <returns></returns>
        public ExcelConfigSet AddKVIndexAuto(string key) { return AddKVIndex(0, item => new Key(item.GetFieldValue(key))); }

        /// <summary>
        /// 添加Key-List型索引
        /// </summary>
        /// <param name="index">索引ID，从0开始，每一组配置都可以添加多个索引。请保持索引ID尽量小</param>
        /// <param name="fn">取索引Key的函数</param>
        /// <returns>自身</returns>
        public ExcelConfigSet AddKLIndex(int index, IndexFunction fn) {
            if (index < 0) {
                throw new ArgumentException("index must not be negetive");
            }

            if (null == fn) {
                throw new ArgumentNullException("IndexFunction");
            }

            while (klIndex.Count <= index) {
                KLIndexData obj = new KLIndexData();
                obj.Handle = null;
                obj.Index = new Dictionary<Key, List<DynamicMessage>>();
                obj.SortRule = null;
                klIndex.Add(obj);
            }

            KLIndexData index_set = klIndex[index];
            index_set.Handle = fn;

            foreach (var data_item in datas) {
                Key key = index_set.Handle(data_item);
                List<DynamicMessage> ls;
                if (index_set.Index.TryGetValue(key, out ls)) {
                    ls.Add(data_item);
                } else {
                    index_set.Index[key] = new List<DynamicMessage>() { data_item };
                }
            }

            return this;
        }

        /// <summary>
        /// 设置Key-List索引的排序规则，索引ID为0
        /// </summary>
        /// <param name="fn">排序函数</param>
        /// <returns>自身</returns>
        public ExcelConfigSet SetKLSortRule(IComparer<DynamicMessage> fn) {
            return SetKLSortRule(0, fn);
        }

        /// <summary>
        /// 设置Key-List索引的排序规则，索引ID为0
        /// </summary>
        /// <param name="index">索引ID</param>
        /// <param name="fn">排序函数</param>
        /// <returns>自身</returns>
        public ExcelConfigSet SetKLSortRule(int index, IComparer<DynamicMessage> fn) {
            if (index < 0) {
                throw new ArgumentException("index must not be negetive");
            }

            if (index >= klIndex.Count) {
                throw new ArgumentException("index extended the ");
            }

            if (null == fn) {
                throw new ArgumentNullException("IndexFunction");
            }

            klIndex[index].SortRule = fn;
            foreach (var index_set in klIndex[index].Index) {
                index_set.Value.Sort(fn);
            }
            return this;
        }

        /// <summary>
        /// 添加过滤器，不符合规则的数据会被忽略
        /// </summary>
        /// <param name="fn">规则函数，返回false表示要忽略</param>
        /// <returns>-1或添加前的过滤器数量</returns>
        public int AddFilter(FilterFunction fn) {
            if (null == fn) {
                return -1;
            }

            filters.Add(fn);
            return filters.Count - 1;
        }

        /// <summary>
        /// 获取Key-Value数据
        /// </summary>
        /// <param name="type">索引ID</param>
        /// <param name="key">Key</param>
        /// <returns>动态消息对象，找不到则返回null</returns>
        public DynamicMessage GetKV(int type, Key key) {
            if (type < 0 || type >= kvIndex.Count) {
                return null;
            }

            DynamicMessage ret;
            if (kvIndex[type].Index.TryGetValue(key, out ret)) {
                return ret;
            }

            return null;
        }

        /// <summary>
        /// 获取Key-Value数据
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns>动态消息对象，找不到则返回null</returns>
        public DynamicMessage GetKV(Key key) {
            return GetKV(0, key);
        }

        /// <summary>
        /// 获取Key-Value数据
        /// </summary>
        /// <param name="objKey">Key</param>
        /// <returns>动态消息对象，找不到则返回null</returns>
        public DynamicMessage GetKVAuto(object objKey) {
            return GetKV(0, new Key(objKey));
        }

        /// <summary>
        /// 获取Key-List数据
        /// </summary>
        /// <param name="type">索引ID</param>
        /// <param name="key">Key</param>
        /// <returns>动态消息对象，找不到则返回null</returns>
        public List<DynamicMessage> GetKL(int type, Key key) {
            if (type < 0 || type >= klIndex.Count) {
                return null;
            }

            List<DynamicMessage> ret;
            if (klIndex[type].Index.TryGetValue(key, out ret)) {
                return ret;
            }

            return null;
        }

        /// <summary>
        /// 获取Key-List数据
        /// </summary>
        /// <param name="key">Key</param>
        /// <returns>动态消息对象，找不到则返回null</returns>
        public List<DynamicMessage> GetKL(Key key) {
            return GetKL(0, key);
        }
    }
}