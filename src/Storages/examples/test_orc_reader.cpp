#include <memory>
#include <Storages/Iceberg/ReadBufferFromHDFS.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/MapConfiguration.h>
#include <Storages/Iceberg/IcebergFileSource.cpp>
#include "arrow/filesystem/hdfs.h"

int main()
{
    using namespace DB;
    std::cout << "begin" << std::endl;
    Poco::AutoPtr<Poco::Util::MapConfiguration> config = new Poco::Util::MapConfiguration();
    config->setString("hdfs.hadoop_kerberos_keytab", "/home/bigdata/bigdata.keytab");
    config->setString("hdfs.hadoop_kerberos_principal", "bigdata@BILIBILI.CO");
    config->setString("hdfs.hadoop_security_kerberos_ticket_cache_path", "/home/bigdata/ticket_cache");
    config->setString("hdfs.hadoop_kerberos_kinit_command", "kinit");
    // config->setString("dfs.client.ec.reconstruction.enabled", "true");

    
    const String hdfs_uri = "viewfs://jssz-bigdata-cluster";
    auto fs = Coordination::getHadoopFS(hdfs_uri, *config);

    std::cout << "get fs" << std::endl;

    // const String hdfs_file_path = "/department/inf/iceberg_bls_warehouse/game_uma_bi_bgame__bi_bili/data/log_date=20250702/log_hour=08/00000-432725-27545718-0d36-4e78-9eab-18d04318283f-00001.orc";
    const String hdfs_file_path = "/test/ck_on_iceberg/00000-432725-27545718-0d36-4e78-9eab-18d04318283f-00001.orc";
    auto read_buffer = std::make_shared<ReadBufferFromHDFS>(fs, hdfs_uri, hdfs_file_path, 130712249, DBMS_DEFAULT_BUFFER_SIZE);
    read_buffer->initRemoteOnlyOnce();
    std::cout << "get read buffer" << std::endl;

    char * buff = new char(2000);
    size_t length = 16384;
    size_t read_bytes = read_buffer->readDirect(buff, 130695865, length);
    if (read_bytes == length)
    {
        std::cout << "read successfully" << std::endl;
        uint64_t postscript_length = buff[length - 1] & 0xff;
        std::cout << "postscript length is " << postscript_length << std::endl;
    }
    else
        std::cout << "read failed" << std::endl;

    delete buff;
    return 0;
}


