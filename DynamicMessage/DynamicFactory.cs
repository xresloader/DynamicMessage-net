using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;

using google.protobuf;
using ProtoBuf;

namespace xresloader.Protobuf {
    public class DynamicFactory {
        private DynamicMessage.DynamicDiscriptors descriptors;
        private LinkedList<string> lastError = null;

        public DynamicMessage.DynamicDiscriptors Descriptors {
            get {
                return descriptors;
            }
        }

        public DynamicFactory() {
            Clear();
        }

        public bool Register(string path) {
            lastError.Clear();

            try {
                using (FileStream stream = File.OpenRead(path)) {
                    return Register(stream);
                }
            } catch (Exception e) {
                lastError.AddLast(e.Message);
                return false;
            }

        }

        public bool Register(Stream source) {
            lastError.Clear();

            FileDescriptorSet desc = new FileDescriptorSet();
            Serializer.Merge<FileDescriptorSet>(source, desc);
            buildFileDescriptor(desc);

            return lastError.Count == 0;
        }

        public string LastError {
            get {
                return string.Join(", ", lastError.ToArray());
            }
        }

        public DynamicMessage.MsgDiscriptor GetMsgDiscriptor(string path) {
            DynamicMessage.MsgDiscriptor out_desc;
            if (descriptors.MsgDescriptors.TryGetValue(path, out out_desc)) {
                return out_desc;
            }
            return null;
        }

        public DynamicMessage Create(DynamicMessage.MsgDiscriptor out_desc) {
            return new DynamicMessage(out_desc, descriptors);
        }

        public DynamicMessage Create(string path) {
            lastError.Clear();

            DynamicMessage.MsgDiscriptor out_desc = GetMsgDiscriptor(path);
            if (null != out_desc) {
                return Create(out_desc);
            }

            lastError.AddLast(string.Format("invalid protocol path {0}", path));
            return null;
        }

        public DynamicMessage Decode(string path, Stream stream) {
            lastError.Clear();

            DynamicMessage ret = Create(path);
            if (null == ret) {
                lastError.AddLast(string.Format("invalid protocol path {0}", path));
                return null;
            }

            if (false == ret.Parse(stream)) {
                lastError.AddLast(ret.LastError);
                return null;
            }
            
            return ret;
        }

        public DynamicMessage Decode(DynamicMessage.MsgDiscriptor out_desc, Stream stream) {
            lastError.Clear();

            DynamicMessage ret = Create(out_desc);
            if (null == ret) {
                lastError.AddLast(string.Format("invalid protocol path {0}.{1}", out_desc.Package, out_desc.Protocol));
                return null;
            }

            if (false == ret.Parse(stream)) {
                lastError.AddLast(ret.LastError);
                return null;
            }

            return ret;
        }

        public bool Encode(DynamicMessage msg, Stream stream) {
            lastError.Clear();
            if (null == msg) {
                lastError.AddLast("DynamicMessage can not be null");
                return false;
            }

            if (false == msg.Serialize(stream)) {
                lastError.AddLast(msg.LastError);
                return false;
            }

            return true;
        }

        public EnumDescriptorProto GetEnumType(string path) {
            EnumDescriptorProto ret;
            if (descriptors.EnumDescriptors.TryGetValue(path, out ret)) {
                return ret;
            }

            return null;
        }

        public EnumValueDescriptorProto GetEnumValue(string path) {
            EnumValueDescriptorProto ret;
            if (descriptors.EnumValueDescriptors.TryGetValue(path, out ret)) {
                return ret;
            }

            return null;
        }

        private void buildFileDescriptor(FileDescriptorSet desc) {
            if (desc == null) throw new ArgumentNullException("FileDescriptorSet can not be null");

            foreach(var fd in desc.file) {
                if (descriptors.FileDescriptors.ContainsKey(fd.name)) {
                    continue;
                }

                descriptors.FileDescriptors.Add(fd.name, fd);
                foreach (var enum_desc in fd.enum_type) {
                    string enum_key = fd.package.Length > 0? string.Format("{0}.{1}", fd.package, enum_desc.name): enum_desc.name;
                    if (descriptors.EnumDescriptors.ContainsKey(enum_key)) {
                        lastError.AddLast(string.Format("enum discriptor {0} already existed"));
                    } else {
                        descriptors.EnumDescriptors.Add(enum_key, enum_desc);

                        foreach (var enum_val in enum_desc.value) {
                            string enum_val_key = string.Format("{0}.{1}", enum_key, enum_val.name);
                            descriptors.EnumValueDescriptors[enum_val_key] = enum_val;
                        }
                    }
                }

                foreach (var msg_desc in fd.message_type) {
                    string key = fd.package.Length > 0 ? string.Format("{0}.{1}", fd.package, msg_desc.name): msg_desc.name;
                    if (descriptors.MsgDescriptors.ContainsKey(key)) {
                        lastError.AddLast(string.Format("message discriptor {0} already existed"));
                    } else {
                        DynamicMessage.MsgDiscriptor res = new DynamicMessage.MsgDiscriptor();
                        res.Package = fd.package;
                        res.Protocol = msg_desc;
                        res.FieldIdIndex = new Dictionary<int, FieldDescriptorProto>();
                        res.FieldNameIndex = new Dictionary<string, FieldDescriptorProto>();
                        descriptors.MsgDescriptors.Add(key, res);
                    }
                }
            }
        }

        public void Clear() {
            descriptors = new DynamicMessage.DynamicDiscriptors();
            descriptors.FileDescriptors = new Dictionary<string, FileDescriptorProto>();
            descriptors.MsgDescriptors = new Dictionary<string, DynamicMessage.MsgDiscriptor>();
            descriptors.EnumDescriptors = new Dictionary<string, EnumDescriptorProto>();
            descriptors.EnumValueDescriptors = new Dictionary<string, EnumValueDescriptorProto>();
            lastError = new LinkedList<string>();
        }
        
        public string DebugString() {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("DynamicFactory: " + this.GetHashCode());

            sb.AppendLine("  File Descroptor List: " + descriptors.FileDescriptors.Count);
            foreach(KeyValuePair<string, FileDescriptorProto > ds in descriptors.FileDescriptors) {
                sb.AppendLine("    File: " + ds.Key);
            }

            sb.AppendLine("  Message Descroptor List: " + descriptors.MsgDescriptors.Count);
            foreach (KeyValuePair<string, DynamicMessage.MsgDiscriptor> ds in descriptors.MsgDescriptors) {
                sb.AppendLine("    Message: " + ds.Key + " with " + ds.Value.Protocol.field.Count + " fields");
            }

            sb.AppendLine("  Enum Descroptor List: " + descriptors.EnumDescriptors.Count);
            foreach (KeyValuePair<string, EnumDescriptorProto> ds in descriptors.EnumDescriptors) {
                sb.AppendLine("    Enum: " + ds.Key + " with " + ds.Value.value.Count + " options");
            }

            return sb.ToString();
        }
    }
}
