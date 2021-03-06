﻿// Licensed to the Apache Software Foundation (ASF) under one
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

using System;
using System.Collections.Generic;
using Org.Apache.REEF.Common.Client.Parameters;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.Client.API
{
    /// <summary>
    /// Instantiates IJobSubmissionBuilder based on configurationProviders.
    /// </summary>
    [Obsolete("Deprecated in 0.14. Please use JobRequestBuilder instead.")]
    public sealed class JobSubmissionBuilderFactory
    {
        private readonly ISet<IConfigurationProvider> _configurationProviders;

        [Inject]
        internal JobSubmissionBuilderFactory(
            [Parameter(typeof(DriverConfigurationProviders))] ISet<IConfigurationProvider> configurationProviders)
        {
            _configurationProviders = configurationProviders;
        }

        /// <summary>
        /// Instantiates IJobSubmissionBuilder
        /// </summary>
        public IJobSubmissionBuilder GetJobSubmissionBuilder()
        {
            return new JobSubmissionBuilder(_configurationProviders);
        }
    }
}