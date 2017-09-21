using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using google.protobuf;
using ProtoBuf;

namespace xresloader.Protobuf {
    public partial class DynamicMessage {
        static readonly UTF8Encoding encoding = new UTF8Encoding();

        public class MsgDiscriptor {
            public string Package;
            public DescriptorProto Protocol;
            public Dictionary<int, FieldDescriptorProto> FieldIdIndex;
            public Dictionary<string, FieldDescriptorProto> FieldNameIndex;
        }

        public struct DynamicDiscriptors {
            public Dictionary<string, FileDescriptorProto> FileDescriptors;
            public Dictionary<string, MsgDiscriptor> MsgDescriptors;
            public Dictionary<string, EnumDescriptorProto> EnumDescriptors;
            public Dictionary<string, EnumValueDescriptorProto> EnumValueDescriptors;
        }

        private DynamicMessage.DynamicDiscriptors descriptors;
        private LinkedList<string> lastError = new LinkedList<string>();
        private MsgDiscriptor msgDescriptor;
        private Dictionary<int, object> fieldData = new Dictionary<int, object>();

        public DynamicMessage(MsgDiscriptor proto, DynamicDiscriptors desc) {
            descriptors = desc;
            msgDescriptor = proto;
        }

        public string LastError {
            get {
                return string.Join(", ", lastError.ToArray());
            }
        }

        private void buildIndex() {
            if (msgDescriptor.FieldIdIndex.Count != msgDescriptor.Protocol.field.Count) {
                msgDescriptor.FieldIdIndex.Clear();
                foreach (var fpb in msgDescriptor.Protocol.field) {
                    msgDescriptor.FieldIdIndex.Add(fpb.number, fpb);
                }
            }

            if (msgDescriptor.FieldNameIndex.Count != msgDescriptor.Protocol.field.Count) {
                msgDescriptor.FieldNameIndex.Clear();
                foreach (var fpb in msgDescriptor.Protocol.field) {
                    msgDescriptor.FieldNameIndex.Add(fpb.name, fpb);
                }
            }
        }

        private object defaultValue(FieldDescriptorProto desc) {
            switch (desc.type) {
                case FieldDescriptorProto.Type.TYPE_DOUBLE:
                    return (double)0.0;
                case FieldDescriptorProto.Type.TYPE_FLOAT:
                    return (float)0.0;
                case FieldDescriptorProto.Type.TYPE_INT64:
                case FieldDescriptorProto.Type.TYPE_SFIXED64:
                case FieldDescriptorProto.Type.TYPE_SINT64:
                    return (long)0;
                case FieldDescriptorProto.Type.TYPE_FIXED64:
                case FieldDescriptorProto.Type.TYPE_UINT64:
                    return (ulong)0;
                case FieldDescriptorProto.Type.TYPE_INT32:
                case FieldDescriptorProto.Type.TYPE_SINT32:
                case FieldDescriptorProto.Type.TYPE_SFIXED32:
                    return (int)0;
                case FieldDescriptorProto.Type.TYPE_FIXED32:
                case FieldDescriptorProto.Type.TYPE_UINT32:
                    return (uint)0;
                case FieldDescriptorProto.Type.TYPE_BOOL:
                    return false;
                case FieldDescriptorProto.Type.TYPE_STRING:
                    return "";
                case FieldDescriptorProto.Type.TYPE_GROUP:
                    return null;
                case FieldDescriptorProto.Type.TYPE_MESSAGE:
                    return null;
                case FieldDescriptorProto.Type.TYPE_BYTES:
                    return new byte[0];
                case FieldDescriptorProto.Type.TYPE_ENUM:
                    return (int)0;
                default:
                    return null;
            }
        }

        public object GetFieldValue(int field_id) {
            buildIndex();

            FieldDescriptorProto field;
            if (msgDescriptor.FieldIdIndex.TryGetValue(field_id, out field)) {
                return GetFieldValue(field);
            }

            lastError.Clear();
            lastError.AddLast(string.Format("message {0} has no field {1}", msgDescriptor.Protocol.name, field_id));
            return null;
        }

        public object GetFieldValue(string field_name) {
            buildIndex();

            FieldDescriptorProto field;
            if (msgDescriptor.FieldNameIndex.TryGetValue(field_name, out field)) {
                return GetFieldValue(field);
            }

            lastError.Clear();
            lastError.AddLast(string.Format("message {0} has no field {1}", msgDescriptor.Protocol.name, field_name));
            return null;
        }

        public object GetFieldValue(FieldDescriptorProto field_desc) {
            lastError.Clear();

            object ret;
            if (fieldData.TryGetValue(field_desc.number, out ret)) {
                if (ret is List<object>) {
                    lastError.AddLast(string.Format("field {0} is repeated", field_desc.number));
                    return null;
                }
                return ret;
            }

            FieldDescriptorProto field;
            if (msgDescriptor.FieldIdIndex.TryGetValue(field_desc.number, out field)) {
                return defaultValue(field);
            }

            return null;
        }

        public List<object> GetFieldList(int field_id) {
            buildIndex();

            FieldDescriptorProto field;
            if (msgDescriptor.FieldIdIndex.TryGetValue(field_id, out field)) {
                return GetFieldList(field);
            }

            lastError.Clear();
            lastError.AddLast(string.Format("message {0} has no field {1}", msgDescriptor.Protocol.name, field_id));
            return null;
        }

        public List<object> GetFieldList(string field_name) {
            buildIndex();

            FieldDescriptorProto field;
            if (msgDescriptor.FieldNameIndex.TryGetValue(field_name, out field)) {
                return GetFieldList(field);
            }

            lastError.Clear();
            lastError.AddLast(string.Format("message {0} has no field {1}", msgDescriptor.Protocol.name, field_name));
            return null;
        }

        public List<object> GetFieldList(FieldDescriptorProto field_desc) {
            lastError.Clear();

            object ret;
            if (fieldData.TryGetValue(field_desc.number, out ret)) {
                if (!(ret is List<object>)) {
                    lastError.AddLast(string.Format("field {1} is not repeated", field_desc.number));
                    return null;
                }

                return (List<object>)ret;
            }

            return new List<object>();
        }

        #region 简化API
        public DynamicMessage GetFieldMessage(FieldDescriptorProto desc) {
            if (desc.type != FieldDescriptorProto.Type.TYPE_MESSAGE) {
                lastError.Clear();
                lastError.AddLast(string.Format("field {0}.{1} is not a message", msgDescriptor.Protocol.name, desc.name));
                return null;
            }


            object ret = GetFieldValue(desc);
            if (ret is DynamicMessage) {
                return (DynamicMessage)ret;
            }
            return null;
        }

        public DynamicMessage GetFieldMessage(string field_name) {
            buildIndex();

            FieldDescriptorProto field;
            if (msgDescriptor.FieldNameIndex.TryGetValue(field_name, out field)) {
                return MutableMessage(field);
            }

            lastError.Clear();
            lastError.AddLast(string.Format("message {0} has no field {1}", msgDescriptor.Protocol.name, field_name));
            return null;
        }

        public DynamicMessage GetFieldMessage(int field_id) {
            buildIndex();

            FieldDescriptorProto field;
            if (msgDescriptor.FieldIdIndex.TryGetValue(field_id, out field)) {
                return MutableMessage(field);
            }

            lastError.Clear();
            lastError.AddLast(string.Format("message {0} has no field {1}", msgDescriptor.Protocol.name, field_id));
            return null;
        }

        #endregion

        /// <summary>
        /// 输出所有有数据的Fields
        /// </summary>
        /// <returns></returns>
        public List<FieldDescriptorProto> ReflectListFields {
            get {
                buildIndex();

                List<FieldDescriptorProto> ret = new List<FieldDescriptorProto>();
                foreach (var data in fieldData) {
                    FieldDescriptorProto desc;
                    if (msgDescriptor.FieldIdIndex.TryGetValue(data.Key, out desc)) {
                        ret.Add(desc);
                    }
                }

                return ret;
            }
        }

        /// <summary>
        /// Message描述信息
        /// </summary>
        public DescriptorProto Descriptor {
            get {
                return msgDescriptor.Protocol;
            }
        }

        /// <summary>
        /// Package名称
        /// </summary>
        public string PackageName {
            get {
                return msgDescriptor.Package;
            }
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

        public bool Parse(Stream stream) {
            buildIndex();
            lastError.Clear();

            try {
                using (ProtoReader reader = new ProtoReader(stream, null, null)) {
                    return Parse(reader);
                }
            } catch (Exception e) {
                lastError.AddLast(e.Message);
            }

            return 0 == lastError.Count;
        }

        private string pickMsgName(FieldDescriptorProto desc) {
            string msg_name = desc.type_name;
            if (msg_name.Length > 0 && '.' == msg_name[0]) {
                msg_name = msg_name.Substring(1);
            }
            return msg_name;
        }

        /// <summary>
        /// 创见一个动态Message，和当前的Message共享同一个类型描述集
        /// </summary>
        /// <param name="path">类型路径</param>
        /// <returns>新创建的Message，如果创建失败返回null</returns>
        public DynamicMessage CreateMessage(string path) {
            MsgDiscriptor sub_desc;
            if (false == descriptors.MsgDescriptors.TryGetValue(path, out sub_desc)) {
                lastError.Clear();
                lastError.AddLast(string.Format("invalid message path {0}", path));
                return null;
            }

            return new DynamicMessage(sub_desc, descriptors);
        }

        /// <summary>
        /// 创见一个动态Message，和当前的Message共享同一个类型描述集
        /// </summary>
        /// <param name="desc">field描述</param>
        /// <returns>新创建的Message，如果创建失败返回null</returns>
        public DynamicMessage CreateMessage(FieldDescriptorProto desc) {
            if (null == desc) {
                lastError.Clear();
                lastError.AddLast("FieldDescriptorProto can not be null");
                return null;
            }

            string msg_name = pickMsgName(desc);
            return CreateMessage(msg_name);
        }

        /// <summary>
        /// 解析protobuf数据
        /// </summary>
        /// <param name="reader"></param>
        /// <see cref="https://developers.google.com/protocol-buffers/docs/encoding"/>
        /// <returns>全部成功返回true，部分失败或全失败返回false，这时候可以通过LastError获取失败信息</returns>
        public bool Parse(ProtoReader reader) {
            buildIndex();
            lastError.Clear();

            try {
                int field_id;
                SubItemToken token;
                while ((field_id = reader.ReadFieldHeader()) != 0) {
                    WireType pb_type = reader.WireType;

                    FieldDescriptorProto desc;
                    if (false == msgDescriptor.FieldIdIndex.TryGetValue(field_id, out desc)) {
                        // unknown field skipped
                        reader.SkipField();
                        continue;
                    }

                    // 类型校验
                    try {
                        switch (desc.type) {
                            case FieldDescriptorProto.Type.TYPE_DOUBLE:
                                insertField(desc, reader.ReadDouble());
                                break;
                            case FieldDescriptorProto.Type.TYPE_FLOAT:
                                insertField(desc, reader.ReadSingle());
                                break;
                            case FieldDescriptorProto.Type.TYPE_INT64:
                            case FieldDescriptorProto.Type.TYPE_SINT64:
                            case FieldDescriptorProto.Type.TYPE_SFIXED64:
                                insertField(desc, reader.ReadInt64());
                                break;
                            case FieldDescriptorProto.Type.TYPE_UINT64:
                            case FieldDescriptorProto.Type.TYPE_FIXED64:
                                insertField(desc, reader.ReadUInt64());
                                break;
                            case FieldDescriptorProto.Type.TYPE_INT32:
                            case FieldDescriptorProto.Type.TYPE_SINT32:
                            case FieldDescriptorProto.Type.TYPE_SFIXED32:
                                insertField(desc, reader.ReadInt32());
                                break;
                            case FieldDescriptorProto.Type.TYPE_BOOL:
                                insertField(desc, reader.ReadBoolean());
                                break;
                            case FieldDescriptorProto.Type.TYPE_STRING:
                                insertField(desc, reader.ReadString());
                                break;
                            //case FieldDescriptorProto.Type.TYPE_GROUP: // deprecated
                            //    break;
                            case FieldDescriptorProto.Type.TYPE_MESSAGE:
                                token = ProtoReader.StartSubItem(reader);
                                try {
                                    DynamicMessage sub_msg = CreateMessage(desc);
                                    if (null == sub_msg) {
                                        lastError.AddLast(string.Format("{0}.{1}.{2} => invalid message path {3}", msgDescriptor.Package, msgDescriptor.Protocol.name, desc.name, pickMsgName(desc)));
                                        break;
                                    }
                                    if (false == sub_msg.Parse(reader)) {
                                        lastError.AddLast(sub_msg.LastError);
                                    } else {
                                        insertField(desc, sub_msg);
                                    }
                                } catch (Exception e) {
                                    lastError.AddLast(string.Format("{0}.{1}.{2} => {3}", msgDescriptor.Package, msgDescriptor.Protocol.name, desc.name, e.Message));
                                }
                                ProtoReader.EndSubItem(token, reader);
                                break;
                            case FieldDescriptorProto.Type.TYPE_BYTES:
                                insertField(desc, ProtoReader.AppendBytes(null, reader));
                                break;
                            case FieldDescriptorProto.Type.TYPE_FIXED32:
                            case FieldDescriptorProto.Type.TYPE_UINT32:
                                insertField(desc, reader.ReadUInt32());
                                break;
                            case FieldDescriptorProto.Type.TYPE_ENUM:
                                insertField(desc, reader.ReadInt32());
                                break;
                            default:
                                // unsupported field
                                lastError.AddLast(string.Format("field type {0} in {1}.{2}.{3} unsupported", desc.type.ToString(), msgDescriptor.Package, msgDescriptor.Protocol.name, desc.name));
                                reader.SkipField();
                                break;
                        }
                    } catch (Exception e) {
                        lastError.AddLast(string.Format("{0}.{1}.{2} {3}", msgDescriptor.Package, msgDescriptor.Protocol.name, desc.name, e.ToString()));
                        reader.SkipField();
                    }
                }
            } catch (Exception e) {
                lastError.AddLast(e.Message);
            }

            return 0 == lastError.Count;
        }

        /// <summary>
        /// 转储数据到Stream中
        /// </summary>
        /// <param name="stream">保存的目标</param>
        /// <returns>成功发回true，部分失败返回false，此时可以通过LastError获取失败信息</returns>
        public bool Serialize(Stream stream) {
            buildIndex();
            lastError.Clear();

            try {
                using (ProtoWriter writer = new ProtoWriter(stream, null, null)) {
                    return Serialize(writer);
                }
            } catch (Exception e) {
                lastError.AddLast(e.Message);
            }

            return 0 == lastError.Count;
        }

        /// <summary>
        /// 写出数据
        /// </summary>
        /// <param name="writer">写出目标</param>
        /// <param name="obj">写出对象</param>
        /// <param name="field_desc">字段描述</param>
        /// <see cref="https://developers.google.com/protocol-buffers/docs/encoding"/>
        private void write_object(ProtoWriter writer, object obj, FieldDescriptorProto field_desc) {
            try {
                switch (field_desc.type) {
                    case FieldDescriptorProto.Type.TYPE_DOUBLE: {
                            double val = Convert.ToDouble(obj);
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.Fixed64, writer);
                            ProtoWriter.WriteDouble(val, writer);
                            break;
                        }
                    case FieldDescriptorProto.Type.TYPE_FLOAT: {
                            float val = Convert.ToSingle(obj);
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.Fixed32, writer);
                            ProtoWriter.WriteSingle(val, writer);
                            break;
                        }
                    case FieldDescriptorProto.Type.TYPE_SINT64:
                    case FieldDescriptorProto.Type.TYPE_INT64: {
                            long val = Convert.ToInt64(obj);
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.Variant, writer);
                            ProtoWriter.WriteInt64(val, writer);
                            break;
                        }
                    case FieldDescriptorProto.Type.TYPE_UINT64: {
                            ulong val = Convert.ToUInt64(obj);
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.Variant, writer);
                            ProtoWriter.WriteUInt64(val, writer);
                            break;
                        }
                    case FieldDescriptorProto.Type.TYPE_SINT32:
                    case FieldDescriptorProto.Type.TYPE_INT32: {
                            int val = Convert.ToInt32(obj);
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.Variant, writer);
                            ProtoWriter.WriteInt32(val, writer);
                            break;
                        }
                    case FieldDescriptorProto.Type.TYPE_FIXED64: {
                            long val = Convert.ToInt64(obj);
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.Fixed64, writer);
                            ProtoWriter.WriteInt64(val, writer);
                            break;
                        }
                    case FieldDescriptorProto.Type.TYPE_FIXED32: {
                            int val = Convert.ToInt32(obj);
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.Fixed32, writer);
                            ProtoWriter.WriteInt32(val, writer);
                            break;
                        }
                    case FieldDescriptorProto.Type.TYPE_BOOL: {
                            bool val = Convert.ToBoolean(obj);
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.Variant, writer);
                            ProtoWriter.WriteBoolean(val, writer);
                            break;
                        }
                    case FieldDescriptorProto.Type.TYPE_STRING: {
                            string val = Convert.ToString(obj);
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.String, writer);
                            ProtoWriter.WriteString(val, writer);
                            break;
                        }
                    //case FieldDescriptorProto.Type.TYPE_GROUP: // deprecated
                    //    break;
                    case FieldDescriptorProto.Type.TYPE_MESSAGE:
                        if (!(obj is DynamicMessage)) {
                            lastError.AddLast(string.Format("try add {0} to {1}.{2}.{3}, but not a message", obj.ToString(), msgDescriptor.Package, msgDescriptor.Protocol.name, field_desc.name));
                            break;
                        }

                        ProtoWriter.WriteFieldHeader(field_desc.number, WireType.String, writer);
                        SubItemToken token = ProtoWriter.StartSubItem(null, writer);
                        try {
                            ((DynamicMessage)obj).Serialize(writer);
                        } catch (Exception e) {
                            lastError.AddLast(string.Format("{0}.{1}.{2} => {3}", msgDescriptor.Package, msgDescriptor.Protocol.name, field_desc.name, e.Message));
                        }
                        ProtoWriter.EndSubItem(token, writer);

                        break;
                    case FieldDescriptorProto.Type.TYPE_BYTES: {
                            if (!(obj is byte[])) {
                                throw new ArgumentException("{0} should be a byte[]", field_desc.name);
                            }
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.String, writer);
                            ProtoWriter.WriteBytes((byte[])obj, writer);
                            break;
                        }
                    case FieldDescriptorProto.Type.TYPE_UINT32: {
                            uint val = Convert.ToUInt32(obj);
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.Variant, writer);
                            ProtoWriter.WriteUInt32(val, writer);
                            break;
                        }
                    case FieldDescriptorProto.Type.TYPE_ENUM: {
                            int val = Convert.ToInt32(obj);
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.Variant, writer);
                            ProtoWriter.WriteInt32(val, writer);
                            break;
                        }
                    case FieldDescriptorProto.Type.TYPE_SFIXED32: {
                            uint val = Convert.ToUInt32(obj);
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.Fixed32, writer);
                            ProtoWriter.WriteUInt32(val, writer);
                            break;
                        }
                    case FieldDescriptorProto.Type.TYPE_SFIXED64: {
                            ulong val = Convert.ToUInt64(obj);
                            ProtoWriter.WriteFieldHeader(field_desc.number, WireType.Fixed64, writer);
                            ProtoWriter.WriteUInt64(val, writer);
                            break;
                        }
                    default:
                        // unsupported field
                        lastError.AddLast(string.Format("field type {0} in {1}.{2}.{3} unsupported", field_desc.type.ToString(), msgDescriptor.Package, msgDescriptor.Protocol.name, field_desc.name));
                        break;
                }
            } catch (Exception e) {
                lastError.AddLast(string.Format("{0}.{1}.{2} {3}", msgDescriptor.Package, msgDescriptor.Protocol.name, field_desc.name, e.ToString()));
            }
        }

        /// <summary>
        /// 转储数据
        /// </summary>
        /// <param name="writer">写出目标</param>
        /// <returns>全部成功返回true，部分失败或全失败返回false，这时候可以通过LastError获取失败信息</returns>
        public bool Serialize(ProtoWriter writer) {
            buildIndex();
            lastError.Clear();

            foreach (var field in fieldData) {
                FieldDescriptorProto field_desc = null;
                // 不认识的值，直接忽略
                if (false == msgDescriptor.FieldIdIndex.TryGetValue(field.Key, out field_desc)) {
                    continue;
                }

                if (field_desc.label == FieldDescriptorProto.Label.LABEL_REPEATED) {
                    foreach (var obj in (List<object>)field.Value) {
                        write_object(writer, obj, field_desc);
                    }
                } else {
                    write_object(writer, field.Value, field_desc);
                }
            }

            return 0 == lastError.Count;
        }

        private void insertField(FieldDescriptorProto desc, object obj) {
            object old_data;
            
            if (desc.label == FieldDescriptorProto.Label.LABEL_REPEATED) {
                bool has_old = fieldData.TryGetValue(desc.number, out old_data);
                if (has_old) {
                    ((List<object>)old_data).Add(obj);
                } else {
                    fieldData.Add(desc.number, new List<object> { obj });
                }
            } else {
                fieldData[desc.number] = obj;
            }
        }

        private void buildString(StringBuilder builder, string ident = "") {
            builder.Append("{\n");
            foreach (var field in fieldData) {
                FieldDescriptorProto field_desc = null;
                if (msgDescriptor.FieldIdIndex.TryGetValue(field.Key, out field_desc)) {
                    builder.AppendFormat("  {0}{1} = ", ident, field_desc.name);
                } else {
                    builder.AppendFormat("  {0}{1} = ", ident, field.Key);
                }

                if (field.Value is List<object>) {
                    builder.Append("[");
                    foreach (var sub_obj in ((List<object>)field.Value)) {
                        builder.AppendFormat(" {0} ", ident);
                        if (sub_obj is DynamicMessage) {
                            ((DynamicMessage)sub_obj).buildString(builder, ident + "    ");
                            builder.Append(",");
                        } else {
                            if (null != field_desc && field_desc.type == FieldDescriptorProto.Type.TYPE_BYTES) {
                                builder.AppendFormat("{0},", BitConverter.ToString((byte[])sub_obj).Replace("-", ""));
                            } else {
                                builder.AppendFormat("{0},", sub_obj.ToString());
                            }
                        }
                    }

                    builder.AppendFormat("{0} ],\n", ident);
                } else {
                    if (field.Value is DynamicMessage) {
                        ((DynamicMessage)field.Value).buildString(builder, ident + "  ");
                        builder.Append(",");
                    } else {
                        if (null != field_desc && field_desc.type == FieldDescriptorProto.Type.TYPE_BYTES) {
                            builder.AppendFormat("{0},", BitConverter.ToString((byte[])field.Value).Replace("-", ""));
                        } else {
                            builder.AppendFormat("{0},", field.Value.ToString());
                        }
                    }
                }
            }
            builder.Append(ident);
            builder.Append("}");
        }

        public override string ToString() {
            StringBuilder builder = new StringBuilder();

            buildString(builder);
            return builder.ToString();
        }

        /// <summary>
        /// 移除字段
        /// </summary>
        /// <param name="field_id">字段ID</param>
        /// <returns>原先不存在返回false，否则返回true</returns>
        public bool RemoveField(int field_id) {
            if (fieldData.ContainsKey(field_id)) {
                fieldData.Remove(field_id);
                return true;
            }

            return false;
        }

        /// <summary>
        /// 移除字段
        /// </summary>
        /// <param name="field_desc">字段描述</param>
        /// <returns>原先不存在返回false，否则返回true</returns>
        public bool RemoveField(FieldDescriptorProto field_desc) {
            return RemoveField(field_desc.number);
        }

        /// <summary>
        /// 移除字段
        /// </summary>
        /// <param name="field_name">字段名称</param>
        /// <returns>原先不存在返回false，否则返回true</returns>
        public bool RemoveField(string field_name) {
            buildIndex();

            FieldDescriptorProto field;
            if (msgDescriptor.FieldNameIndex.TryGetValue(field_name, out field)) {
                return RemoveField(field);
            }

            lastError.Clear();
            lastError.AddLast(string.Format("message {0} has no field {1}", msgDescriptor.Protocol.name, field_name));
            return false;
        }

        private bool verifyField(FieldDescriptorProto desc, ref object obj) {
            switch (desc.type) {
                case FieldDescriptorProto.Type.TYPE_DOUBLE:
                    return obj is double || obj is float;
                case FieldDescriptorProto.Type.TYPE_FLOAT:
                    return obj is double || obj is float;
                case FieldDescriptorProto.Type.TYPE_INT64:
                case FieldDescriptorProto.Type.TYPE_SFIXED64:
                case FieldDescriptorProto.Type.TYPE_SINT64:
                    if (obj is ulong || obj is uint || obj is ushort || obj is byte || obj is long || obj is int || obj is short || obj is sbyte)
                    {
                        return true;
                    }
                    else if (obj is double || obj is float || obj is string) {
                        obj = Convert.ToInt64(obj);
                        return true;
                    }
                    return false;
                case FieldDescriptorProto.Type.TYPE_UINT64:
                case FieldDescriptorProto.Type.TYPE_FIXED64:
                    if (obj is ulong || obj is uint || obj is ushort || obj is byte || obj is long || obj is int || obj is short || obj is sbyte)
                    {
                        return true;
                    }
                    else if (obj is double || obj is float || obj is string)
                    {
                        obj = Convert.ToUInt64(obj);
                        return true;
                    }
                    return false;
                case FieldDescriptorProto.Type.TYPE_ENUM:
                    if (obj is ulong || obj is uint || obj is ushort || obj is byte || obj is long || obj is int || obj is short || obj is sbyte || obj is string)
                    {
                        return true;
                    }
                    else if (obj is double || obj is float)
                    {
                        obj = Convert.ToInt32((double)obj);
                        return true;
                    }
                    return false;
                case FieldDescriptorProto.Type.TYPE_INT32:
                case FieldDescriptorProto.Type.TYPE_SINT32:
                case FieldDescriptorProto.Type.TYPE_SFIXED32:
                    if (obj is ulong || obj is uint || obj is ushort || obj is byte || obj is long || obj is int || obj is short || obj is sbyte)
                    {
                        return true;
                    }
                    else if (obj is double || obj is float || obj is string)
                    {
                        obj = Convert.ToInt32(obj);
                        return true;
                    }
                    return false;
                case FieldDescriptorProto.Type.TYPE_FIXED32:
                case FieldDescriptorProto.Type.TYPE_UINT32:
                    if (obj is ulong || obj is uint || obj is ushort || obj is byte || obj is long || obj is int || obj is short || obj is sbyte)
                    {
                        return true;
                    }
                    else if (obj is double || obj is float || obj is string)
                    {
                        obj = Convert.ToUInt32(obj);
                        return true;
                    }
                    return false;
                case FieldDescriptorProto.Type.TYPE_BOOL:
                    return obj is bool;
                case FieldDescriptorProto.Type.TYPE_STRING:
                    return obj is string;
                case FieldDescriptorProto.Type.TYPE_GROUP://
                    return false;
                case FieldDescriptorProto.Type.TYPE_MESSAGE:
                    return obj is DynamicMessage && pickMsgName(desc) == string.Format("{0}.{1}", ((DynamicMessage)obj).PackageName, ((DynamicMessage)obj).Descriptor.name);
                case FieldDescriptorProto.Type.TYPE_BYTES:
                    return obj is byte[];
                default:
                    return false;
            }
        }

        /// <summary>
        /// 设置值类型数据
        /// </summary>
        /// <param name="desc">key的描述信息</param>
        /// <param name="obj">值</param>
        /// <returns>返回是否成功</returns>
        public bool SetFieldValue(FieldDescriptorProto desc, object obj) {
            if (null == desc) {
                return false;
            }

            buildIndex();
            lastError.Clear();


            if (desc.label == FieldDescriptorProto.Label.LABEL_REPEATED) {
                lastError.AddLast(string.Format("field {0} is repeated, can only use AddFieldList", desc.name));
                return false;
            }

            if (!verifyField(desc, ref obj)) {
                lastError.AddLast(string.Format("field {0} type error, must match {1}", desc.name, desc.type.ToString()));
                return false;
            }

            // 对枚举类型特殊处理
            if (desc.type == FieldDescriptorProto.Type.TYPE_ENUM && obj is string) {
                EnumValueDescriptorProto enum_desc;
                if (!descriptors.EnumValueDescriptors.TryGetValue((string)obj, out enum_desc)) {
                    lastError.AddLast(string.Format("enum value path {0} not found", obj));
                    return false;
                }

                obj = enum_desc.number;
            }

            fieldData[desc.number] = obj;
            return true;
        }

        /// <summary>
        /// 设置值类型数据
        /// </summary>
        /// <param name="desc">key的ID</param>
        /// <param name="obj">值</param>
        /// <returns>返回是否成功</returns>
        public bool SetFieldValue(int field_id, object obj) {
            buildIndex();
            lastError.Clear();

            FieldDescriptorProto desc;
            if (msgDescriptor.FieldIdIndex.TryGetValue(field_id, out desc)) {
                return SetFieldValue(desc, obj);
            } else {
                lastError.AddLast(string.Format("field {0} not found", field_id));
                return false;
            }
        }

        /// <summary>
        /// 设置值类型数据
        /// </summary>
        /// <param name="desc">key的名称</param>
        /// <param name="obj">值</param>
        /// <returns>返回是否成功</returns>
        public bool SetFieldValue(string field_name, object obj) {
            buildIndex();
            lastError.Clear();

            FieldDescriptorProto desc;
            if (msgDescriptor.FieldNameIndex.TryGetValue(field_name, out desc)) {
                return SetFieldValue(desc, obj);
            } else {
                lastError.AddLast(string.Format("field {0} not found", field_name));
                return false;
            }
        }

        /// <summary>
        /// 添加List类型数据
        /// </summary>
        /// <param name="desc">key的描述信息</param>
        /// <param name="obj">值</param>
        /// <returns>返回是否成功</returns>
        public bool AddFieldList(FieldDescriptorProto desc, object obj) {
            if (null == desc) {
                return false;
            }

            buildIndex();
            lastError.Clear();

            if (desc.label != FieldDescriptorProto.Label.LABEL_REPEATED) {
                lastError.AddLast(string.Format("field {0} is repeated, can only use SetFieldValue", desc.name));
                return false;
            }

            if (!verifyField(desc, ref obj)) {
                lastError.AddLast(string.Format("field {0} type error, must match {1}", desc.name, desc.type.ToString()));
                return false;
            }

            // 对枚举类型特殊处理
            if (desc.type == FieldDescriptorProto.Type.TYPE_ENUM && obj is string) {
                EnumValueDescriptorProto enum_desc;
                if (!descriptors.EnumValueDescriptors.TryGetValue((string)obj, out enum_desc)) {
                    lastError.AddLast(string.Format("enum value path {0} not found", obj));
                    return false;
                }

                obj = enum_desc.number;
            }

            object old_val;
            if (fieldData.TryGetValue(desc.number, out old_val)) {
                ((List<object>)old_val).Add(obj);
            } else {
                fieldData.Add(desc.number, new List<object> { obj });
            }
            return true;
        }

        /// <summary>
        /// 添加List类型数据
        /// </summary>
        /// <param name="desc">key的ID</param>
        /// <param name="obj">值</param>
        /// <returns>返回是否成功</returns>
        public bool AddFieldList(int field_id, object obj) {
            buildIndex();
            lastError.Clear();

            FieldDescriptorProto desc;
            if (msgDescriptor.FieldIdIndex.TryGetValue(field_id, out desc)) {
                return AddFieldList(desc, obj);
            } else {
                lastError.AddLast(string.Format("field {0} not found", field_id));
                return false;
            }
        }

        /// <summary>
        /// 添加List类型数据
        /// </summary>
        /// <param name="desc">key的名称</param>
        /// <param name="obj">值</param>
        /// <returns>返回是否成功</returns>
        public bool AddFieldList(string field_name, object obj) {
            buildIndex();
            lastError.Clear();

            FieldDescriptorProto desc;
            if (msgDescriptor.FieldNameIndex.TryGetValue(field_name, out desc)) {
                return AddFieldList(desc, obj);
            } else {
                lastError.AddLast(string.Format("field {0} not found", field_name));
                return false;
            }
        }

        /// <summary>
        /// 添加字段，如果是repeated会追加的末尾，否则是覆盖
        /// </summary>
        /// <param name="desc">key的描述信息</param>
        /// <param name="obj">值</param>
        /// <returns>返回是否成功</returns>
        public bool AddField(FieldDescriptorProto desc, object obj) {
            if (null == desc) {
                return false;
            }

            if (desc.label == FieldDescriptorProto.Label.LABEL_REPEATED) {
                return AddFieldList(desc, obj);
            }

            return SetFieldValue(desc, obj);
        }

        /// <summary>
        /// 添加字段，如果是repeated会追加的末尾，否则是覆盖
        /// </summary>
        /// <param name="desc">key的ID</param>
        /// <param name="obj">值</param>
        /// <returns>返回是否成功</returns>
        public bool AddField(int field_id, object obj) {
            buildIndex();
            lastError.Clear();

            FieldDescriptorProto desc;
            if (msgDescriptor.FieldIdIndex.TryGetValue(field_id, out desc)) {
                return AddField(desc, obj);
            } else {
                lastError.AddLast(string.Format("field {0} not found", field_id));
                return false;
            }
        }

        /// <summary>
        /// 添加字段，如果是repeated会追加的末尾，否则是覆盖
        /// </summary>
        /// <param name="desc">key的名称</param>
        /// <param name="obj">值</param>
        /// <returns>返回是否成功</returns>
        public bool AddField(string field_name, object obj) {
            buildIndex();
            lastError.Clear();

            FieldDescriptorProto desc;
            if (msgDescriptor.FieldNameIndex.TryGetValue(field_name, out desc)) {
                return AddField(desc, obj);
            } else {
                lastError.AddLast(string.Format("field {0} not found", field_name));
                return false;
            }
        }

        #region 简化API
        public DynamicMessage MutableMessage(FieldDescriptorProto desc) {
            if (desc.type != FieldDescriptorProto.Type.TYPE_MESSAGE) {
                lastError.Clear();
                lastError.AddLast(string.Format("field {0} is not a message", desc.type_name));
                return null;
            }

            DynamicMessage ret = GetFieldMessage(desc);
            if (null != ret) {
                return ret;
            }

            ret = CreateMessage(desc);
            if (AddField(desc, ret)) {
                return ret;
            }
            return null;
        }

        public DynamicMessage MutableMessage(string field_name) {
            buildIndex();

            FieldDescriptorProto field;
            if (msgDescriptor.FieldNameIndex.TryGetValue(field_name, out field)) {
                return MutableMessage(field);
            }

            lastError.Clear();
            lastError.AddLast(string.Format("message {0} has no field {1}", msgDescriptor.Protocol.name, field_name));
            return null;
        }

        public DynamicMessage MutableMessage(int field_id) {
            buildIndex();

            FieldDescriptorProto field;
            if (msgDescriptor.FieldIdIndex.TryGetValue(field_id, out field)) {
                return MutableMessage(field);
            }

            lastError.Clear();
            lastError.AddLast(string.Format("message {0} has no field {1}", msgDescriptor.Protocol.name, field_id));
            return null;
        }

        #endregion
    }
}
