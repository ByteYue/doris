// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "recycler/s3_accessor.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/LifecycleRule.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <algorithm>
#include <execution>
#include <type_traits>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "common/sync_point.h"
#include "rate-limiter/s3_rate_limiter.h"
#include "recycler/obj_store_accessor.h"

namespace doris::cloud {

struct AccessorRateLimiter {
public:
    ~AccessorRateLimiter() = default;
    static AccessorRateLimiter& instance();
    S3RateLimiterHolder* rate_limiter(S3RateLimitType type);

private:
    AccessorRateLimiter();
    std::array<std::unique_ptr<S3RateLimiterHolder>, 2> _rate_limiters;
};

[[maybe_unused]] static Aws::Client::AWSError<Aws::S3::S3Errors> s3_error_factory() {
    return {Aws::S3::S3Errors::INTERNAL_FAILURE, "exceeds limit", "exceeds limit", false};
}

template <typename Func>
auto do_s3_rate_limit(S3RateLimitType type, Func callback) -> decltype(callback()) {
    using T = decltype(callback());
    if (!config::enable_s3_rate_limiter) {
        return callback();
    }
    auto sleep_duration = AccessorRateLimiter::instance().rate_limiter(type)->add(1);
    if (sleep_duration < 0) {
        return T(s3_error_factory());
    }
    return callback();
}

template <typename Func>
auto s3_get_rate_limit(Func callback) -> decltype(callback()) {
    using T = decltype(callback());
    if (!config::enable_s3_rate_limiter) {
        return callback();
    }
    auto sleep_duration =
            AccessorRateLimiter::instance().rate_limiter(S3RateLimitType::GET)->add(1);
    if (sleep_duration < 0) {
        return T(-1);
    }
    return callback();
}

template <typename Func>
auto s3_put_rate_limit(Func callback) -> decltype(callback()) {
    using T = decltype(callback());
    if (!config::enable_s3_rate_limiter) {
        return callback();
    }
    auto sleep_duration =
            AccessorRateLimiter::instance().rate_limiter(S3RateLimitType::PUT)->add(1);
    if (sleep_duration < 0) {
        return T(-1);
    }
    return callback();
}

#ifndef UNIT_TEST
#define HELP_MACRO(ret, req, point_name)
#else
#define HELP_MACRO(ret, req, point_name)                       \
    do {                                                       \
        std::pair p {&ret, &req};                              \
        [[maybe_unused]] auto ret_pair = [&p]() mutable {      \
            TEST_SYNC_POINT_RETURN_WITH_VALUE(point_name, &p); \
            return p;                                          \
        }();                                                   \
        return ret;                                            \
    } while (false);
#endif
#define SYNC_POINT_HOOK_RETURN_VALUE(expr, request, point_name, type) \
    [&]() -> decltype(auto) {                                         \
        using T = decltype((expr));                                   \
        [[maybe_unused]] T t;                                         \
        HELP_MACRO(t, request, point_name)                            \
        return do_s3_rate_limit(type, [&]() { return (expr); });      \
    }()

AccessorRateLimiter::AccessorRateLimiter()
        : _rate_limiters {std::make_unique<S3RateLimiterHolder>(
                                  S3RateLimitType::GET, config::s3_get_token_per_second,
                                  config::s3_get_bucket_tokens, config::s3_get_token_limit),
                          std::make_unique<S3RateLimiterHolder>(
                                  S3RateLimitType::PUT, config::s3_put_token_per_second,
                                  config::s3_put_bucket_tokens, config::s3_put_token_limit)} {}

S3RateLimiterHolder* AccessorRateLimiter::rate_limiter(S3RateLimitType type) {
    CHECK(type == S3RateLimitType::GET || type == S3RateLimitType::PUT) << to_string(type);
    return _rate_limiters[static_cast<size_t>(type)].get();
}

AccessorRateLimiter& AccessorRateLimiter::instance() {
    static AccessorRateLimiter instance;
    return instance;
}

int reset_s3_rate_limiter(S3RateLimitType type, size_t max_speed, size_t max_burst, size_t limit) {
    if (type == S3RateLimitType::UNKNOWN) {
        return -1;
    }
    if (type == S3RateLimitType::GET) {
        max_speed = (max_speed == 0) ? config::s3_get_token_per_second : max_speed;
        max_burst = (max_burst == 0) ? config::s3_get_bucket_tokens : max_burst;
        limit = (limit == 0) ? config::s3_get_token_limit : limit;
    } else {
        max_speed = (max_speed == 0) ? config::s3_put_token_per_second : max_speed;
        max_burst = (max_burst == 0) ? config::s3_put_bucket_tokens : max_burst;
        limit = (limit == 0) ? config::s3_put_token_limit : limit;
    }
    return AccessorRateLimiter::instance().rate_limiter(type)->reset(max_speed, max_burst, limit);
}

class S3Environment {
public:
    S3Environment() { Aws::InitAPI(aws_options_); }

    ~S3Environment() { Aws::ShutdownAPI(aws_options_); }

private:
    Aws::SDKOptions aws_options_;
};

S3Accessor::S3Accessor(S3Conf conf) : ObjStoreAccessor(AccessorType::S3), conf_(std::move(conf)) {
    path_ = conf_.endpoint + '/' + conf_.bucket + '/' + conf_.prefix;
}

S3Accessor::~S3Accessor() = default;

std::string S3Accessor::get_key(const std::string& relative_path) const {
    return conf_.prefix + '/' + relative_path;
}

std::string S3Accessor::get_relative_path(const std::string& key) const {
    return key.find(conf_.prefix + "/") != 0 ? "" : key.substr(conf_.prefix.length() + 1);
}

int S3Accessor::init() {
    static S3Environment s3_env;
    Aws::Auth::AWSCredentials aws_cred(conf_.ak, conf_.sk);
    Aws::Client::ClientConfiguration aws_config;
    aws_config.endpointOverride = conf_.endpoint;
    aws_config.region = conf_.region;
    aws_config.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(
            /*maxRetries = 10, scaleFactor = 25*/);
    s3_client_ = std::make_shared<Aws::S3::S3Client>(
            std::move(aws_cred), std::move(aws_config),
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            true /* useVirtualAddressing */);
    return 0;
}

int S3Accessor::delete_objects_by_prefix(const std::string& relative_path) {
    return s3_get_rate_limit([&]() {
               return obj_client_->RecursiveDelete(
                       {.bucket = conf_.bucket, .prefix = get_key(relative_path)});
           })
            .ret;
}

int S3Accessor::delete_objects(const std::vector<std::string>& relative_paths) {
    if (relative_paths.empty()) {
        return 0;
    }
    // `DeleteObjectsRequest` can only contain 1000 keys at most.
    constexpr size_t max_delete_batch = 1000;
    auto path_iter = relative_paths.begin();

    do {
        Aws::Vector<std::string> objects;
        auto path_begin = path_iter;
        for (; path_iter != relative_paths.end() && (path_iter - path_begin < max_delete_batch);
             ++path_iter) {
            auto key = get_key(*path_iter);
            LOG_INFO("delete object")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("key", key)
                    .tag("size", objects.size());
            objects.emplace_back(std::move(key));
        }
        if (objects.empty()) {
            return 0;
        }
        if (auto delete_resp = s3_put_rate_limit([&]() {
                return obj_client_->DeleteObjects({.bucket = conf_.bucket}, std::move(objects));
            });
            delete_resp.ret != 0) {
            return delete_resp.ret != 0;
        }
    } while (path_iter != relative_paths.end());

    return 0;
}

int S3Accessor::delete_object(const std::string& relative_path) {
    return s3_put_rate_limit([&]() {
        return obj_client_->DeleteObject({.bucket = conf_.bucket, .key = get_key(relative_path)})
                .ret;
    });
}

int S3Accessor::put_object(const std::string& relative_path, const std::string& content) {
    return s3_put_rate_limit([&]() {
        return obj_client_
                ->PutObject({.bucket = conf_.bucket, .key = get_key(relative_path)}, content)
                .ret;
    });
}

int S3Accessor::list(const std::string& relative_path, std::vector<ObjectMeta>* files) {
    return s3_get_rate_limit([&]() {
        return obj_client_
                ->ListObjects({.bucket = conf_.bucket, .prefix = get_key(relative_path)}, files)
                .ret;
    });
}

int S3Accessor::exist(const std::string& relative_path) {
    return s3_get_rate_limit([&]() {
        return obj_client_->HeadObject({.bucket = conf_.bucket, .key = get_key(relative_path)}).ret;
    });
}

int S3Accessor::delete_expired_objects(const std::string& relative_path, int64_t expired_time) {
    return s3_put_rate_limit([&]() {
        return obj_client_
                ->DeleteExpired(
                        {.path_opts = {.bucket = conf_.bucket, .prefix = get_key(relative_path)},
                         .relative_path_factory =
                                 [&](const std::string& key) { return get_relative_path(key); }},
                        expired_time)
                .ret;
    });
}

int S3Accessor::get_bucket_lifecycle(int64_t* expiration_days) {
    return s3_get_rate_limit([&]() {
        return obj_client_->GetLifeCycle({.bucket = conf_.bucket}, expiration_days).ret;
    });
}

int S3Accessor::check_bucket_versioning() {
    return s3_get_rate_limit(
            [&]() { return obj_client_->CheckVersioning({.bucket = conf_.bucket}).ret; });
}

int GcsAccessor::delete_objects(const std::vector<std::string>& relative_paths) {
    std::vector<int> delete_rets(relative_paths.size());
    std::transform(std::execution::par, relative_paths.begin(), relative_paths.end(),
                   delete_rets.begin(),
                   [this](const std::string& path) { return delete_object(path); });
    int ret = 0;
    for (int delete_ret : delete_rets) {
        if (delete_ret != 0) {
            ret = delete_ret;
            break;
        }
    }
    return ret;
}
#undef SYNC_POINT_HOOK_RETURN_VALUE
#undef HELP_MACRO
} // namespace doris::cloud
