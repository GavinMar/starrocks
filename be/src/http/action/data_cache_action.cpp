// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "http/action/data_cache_action.h"

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <string>

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"

namespace starrocks {

const static std::string HEADER_JSON = "application/json";
const static std::string ACTION_KEY = "action";
const static std::string ACTION_STAT = "stat";
const static std::string ACTION_INVALIDATE_ALL = "invalidate_all";

void DataCacheAction::handle(HttpRequest* req) {
    VLOG_ROW << req->debug_string();
    const auto& action = req->param(ACTION_KEY);
    if (req->method() == HttpMethod::GET) {
        if (action == ACTION_STAT) {
            _handle_stat(req);
        } else {
            _handle_error(req, strings::Substitute("Not support GET method: '$0'", req->uri()));
        }
    } else {
        _handle_error(req,
                      strings::Substitute("Not support $0 method: '$1'", to_method_desc(req->method()), req->uri()));
    }
}
void DataCacheAction::_handle(HttpRequest* req, const std::function<void(rapidjson::Document&)>& func) {
    rapidjson::Document root;
    root.SetObject();
    func(root);
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, strbuf.GetString());
}
void DataCacheAction::_handle_stat(HttpRequest* req) {
    auto block_cache = _exec_env->block_cache();
    _handle(req, [=](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        if (cache_mgr == nullptr) {
            root.AddMember("error", rapidjson::StringRef("Cache Manager is nullptr"), allocator);
            return;
        }

        auto metrics = block_cache->metrics();
        root.AddMember("status", rapidjson::StringRef(metrics->status), allocator);
        root.AddMember("mem_quota_bytes", rapidjson::Value(metrics->mem_quota_bytes), allocator);
        root.AddMember("mem_used_bytes", rapidjson::Value(metrics->mem_used_bytes), allocator);
        root.AddMember("disk_quota_bytes", rapidjson::Value(metrics->disk_quota_bytes), allocator);
        root.AddMember("disk_used_bytes", rapidjson::Value(metrics->disk_used_bytes), allocator);

        auto mem_used_ratio = metrics->mem_quota_bytes == 0 ? 0.0 : double(metrics->mem_used_bytes) / double(metrics->mem_quota_bytes);
        auto disk_used_ratio = metrics->disk_quota_bytes == 0 ? 0.0 : double(metrics->disk_used_bytes) / double(metrics->disk_quota_bytes);
        root.AddMember("mem_used_ratio", rapidjson::Value(mem_used_ratio), allocator);
        root.AddMember("disk_used_ratio", rapidjson::Value(disk_used_ratio), allocator);
        
        std::string disk_spaces;
        for (size_t i = 0; i < metrics->disk_dir_spaces.size(); ++i) {
            std::string space = fmt::format("{}:{}", metrics->disk_dir_spaces[i].path,
                    metrics->disk_dir_spaces[i].quota_bytes);
            if (i != metrics->disk_dir_spaces.size() - 1) {
                space.append(';');
            }
        }
        root.AddMember("disk_spaces", rapidjson::StringRef(space), allocator);
    });
}

void QueryCacheAction::_handle_invalidate_all(HttpRequest* req) {
    auto cache_mgr = _exec_env->cache_mgr();
    _handle(req, [&](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        if (cache_mgr == nullptr) {
            root.AddMember("error", rapidjson::StringRef("Cache Manager is nullptr"), allocator);
            return;
        }
        cache_mgr->invalidate_all();
        root.AddMember("status", rapidjson::StringRef("OK"), allocator);
    });
}

void QueryCacheAction::_handle_error(HttpRequest* req, const std::string& err_msg) {
    _handle(req, [err_msg](rapidjson::Document& root) {
        auto& allocator = root.GetAllocator();
        root.AddMember("error", rapidjson::Value(err_msg.c_str(), err_msg.size()), allocator);
    });
}

} // namespace starrocks
