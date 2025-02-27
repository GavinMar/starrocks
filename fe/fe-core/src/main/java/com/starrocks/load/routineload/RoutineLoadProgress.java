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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/routineload/RoutineLoadProgress.java

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

package com.starrocks.load.routineload;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class RoutineLoadProgress implements Writable {

    @SerializedName("loadDataSourceType")
    protected LoadDataSourceType loadDataSourceType;
    protected boolean isTypeRead = false;

    public void setTypeRead(boolean isTypeRead) {
        this.isTypeRead = isTypeRead;
    }

    public RoutineLoadProgress(LoadDataSourceType loadDataSourceType) {
        this.loadDataSourceType = loadDataSourceType;
    }

    abstract void update(RLTaskTxnCommitAttachment attachment);

    abstract String toJsonString();

    public static RoutineLoadProgress read(DataInput in) throws IOException {
        RoutineLoadProgress progress = null;
        LoadDataSourceType type = LoadDataSourceType.valueOf(Text.readString(in));
        if (type == LoadDataSourceType.KAFKA) {
            progress = new KafkaProgress();
        } else if (type == LoadDataSourceType.PULSAR) {
            progress = new PulsarProgress();
        } else {
            throw new IOException("Unknown load data source type: " + type.name());
        }

        progress.setTypeRead(true);
        progress.readFields(in);
        return progress;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // ATTN: must write type first
        Text.writeString(out, loadDataSourceType.name());
    }

    public void readFields(DataInput in) throws IOException {
        if (!isTypeRead) {
            loadDataSourceType = LoadDataSourceType.valueOf(Text.readString(in));
            isTypeRead = true;
        }
    }
}
