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

#pragma once

#include <memory>

#include "recycler/obj_store_accessor.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace doris::cloud {

class S3ObjClient : public ObjStorageClient {
public:
    S3ObjClient(std::shared_ptr<Aws::S3::S3Client> client) : s3_client_(std::move(client)) {}
    ~S3ObjClient() override = default;

    ObjectStorageResponse PutObject(const ObjectStoragePathOptions& opts,
                                    std::string_view stream) override;
    ObjectStorageResponse HeadObject(const ObjectStoragePathOptions& opts) override;
    ObjectStorageResponse ListObjects(const ObjectStoragePathOptions& opts,
                                      std::vector<ObjectMeta>* files) override;
    ObjectStorageResponse DeleteObjects(const ObjectStoragePathOptions& opts,
                                        std::vector<std::string> objs) override;
    ObjectStorageResponse DeleteObject(const ObjectStoragePathOptions& opts) override;
    ObjectStorageResponse RecursiveDelete(const ObjectStoragePathOptions& opts) override;
    ObjectStorageResponse DeleteExpired(const ObjectStorageDeleteExpiredOptions& opts,
                                        int64_t expired_time) override;
    ObjectStorageResponse GetLifeCycle(const ObjectStoragePathOptions& opts,
                                       int64_t* expiration_days) override;

    ObjectStorageResponse CheckVersioning(const ObjectStoragePathOptions& opts) override;

    const std::shared_ptr<Aws::S3::S3Client>& s3_client() { return s3_client_; }

private:
    std::shared_ptr<Aws::S3::S3Client> s3_client_;
};

} // namespace doris::cloud