#pragma once


#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>

namespace DB
{

class StatusProcesses
{
public:
    struct ProcessStatus
    {
        std::string info;
        size_t process_status_sign;
    };

    using ProcessStatusPtr = std::shared_ptr<ProcessStatus>;

    static std::string serializeToJSON(ProcessStatusPtr statuses)
    {
        Poco::JSON::Object result_json;
        result_json.set("process_status_sign", statuses->process_status_sign);
        result_json.set("info", statuses->info);

        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(result_json, oss);
        auto result = oss.str();
        return result;
    }
};

}
