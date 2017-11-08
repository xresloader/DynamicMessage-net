# 动态Message支持

+ 目前不支持 packed=true
+ 注意如果Key类型是uint32，那么取的时候Key=10001时要传10001U而不是10001
+ [protobuf-net][1]目录是导入了[protobuf-net][1]核心部分的代码。业务也可以用自己导入的[protobuf-net][1]。
+ [DynamicMessage](DynamicMessage)是动态Message的支持和相关工厂代码
+ [ExcelConfig](ExcelConfig)是读取[xresloader][2]的sample导出数据的高级封装库代码，附带Key-Value和Key-List索引功能。支持多索引
+ [SampleData](SampleData)是[xresloader][2]的sample导出数据和所使用的pb文件
+ [SampleDotnetCore](SampleDotnetCore)是示例代码的.Net Core工程
+ [SampleDotnetFramework](SampleDotnetFramework)是示例代码的.Net Framework工程

[1]: https://github.com/mgravell/protobuf-net
[2]: https://github.com/xresloader/xresloader