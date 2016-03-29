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

using System;
using System.Collections.Generic;
using System.Linq;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.Common.Services;
using Org.Apache.REEF.IMRU.OnREEF.MapInputWithControlMessage;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Network.Group.Config;
using Org.Apache.REEF.Network.Group.Driver;
using Org.Apache.REEF.Tang.Implementations.Configuration;
using Org.Apache.REEF.Tang.Implementations.Tang;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    /// <summary>
    /// Class that handles failed evaluators and also provides Service 
    /// and Context configuration
    /// </summary>
    /// <typeparam name="TMapInput"></typeparam>
    /// <typeparam name="TMapOutput"></typeparam>
    /// <typeparam name="TDataHandler"></typeparam>
    internal sealed class ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TDataHandler>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(ServiceAndContextConfigurationProvider<TMapInput, TMapOutput, TDataHandler>));

        private readonly Dictionary<string, EvaluatorConfigurations> _configurationProvider;
        private readonly ContextAndServiceConfiguration _masterGroupCommConfiguration;
        private bool _masterFailed = false;
        private string _masterEvaluatorId = null;
        private readonly ISet<string> _failedEvaluators;
        private readonly ISet<string> _submittedEvaluators;
        private readonly ISet<string> _dataLoadedEvaluators; 
        private readonly object _lock;
        private readonly int _numNodes;
        private int _assignedPartitionDescriptors;
        private readonly IGroupCommDriver _groupCommDriver;
        private readonly ConfigurationManager _configurationManager;
        private readonly Stack<IPartitionDescriptor> _partitionDescriptors;

        internal ServiceAndContextConfigurationProvider(int numNodes, IGroupCommDriver groupCommDriver,
            ConfigurationManager configurationManager, Stack<IPartitionDescriptor> partitionDescriptors)
        {
            _configurationProvider = new Dictionary<string, EvaluatorConfigurations>();
            _failedEvaluators = new HashSet<string>();
            _submittedEvaluators = new HashSet<string>();
            _dataLoadedEvaluators = new HashSet<string>();
            _numNodes = numNodes;
            _groupCommDriver = groupCommDriver;
            _configurationManager = configurationManager;
            _assignedPartitionDescriptors = 0;
            _partitionDescriptors = partitionDescriptors;
            _lock = new object();
            _masterGroupCommConfiguration = GetUpdateTaskGroupCommContextAndServiceConfiguration();
        }

        internal bool IsReadyForTask(string evaluatorId)
        {
            if (!_submittedEvaluators.Contains(evaluatorId) && !_dataLoadedEvaluators.Contains(evaluatorId))
            {
                Exceptions.Throw(
                    new Exception("Evaluator present neither in data loading stage nor in group comm. stage"),
                    Logger);
            }
            return _dataLoadedEvaluators.Contains(evaluatorId);
        }

        /// <summary>
        /// Handles failed evaluator. Moves the id from 
        /// submitted evaluator to failed evaluator
        /// </summary>
        /// <param name="evaluatorId"></param>
        internal void EvaluatorFailed(string evaluatorId)
        {
            lock (_lock)
            {
                if (!_submittedEvaluators.Contains(evaluatorId) && !_dataLoadedEvaluators.Contains(evaluatorId))
                {
                    Exceptions.Throw(new Exception("Failed evaluator was never submitted"), Logger);
                }

                if (evaluatorId.Equals(_masterEvaluatorId))
                {
                    _masterFailed = true;
                }
                else
                {
                    if (_submittedEvaluators.Contains(evaluatorId))
                    {
                        _submittedEvaluators.Remove(evaluatorId);
                    }
                    _failedEvaluators.Add(evaluatorId);
                   
                    if (_dataLoadedEvaluators.Contains(evaluatorId))
                    {
                        Exceptions.Throw(new Exception("Cannot handle failed evaluators in Group Comm. service level"),
                            Logger);
                    }
                }
            }
        }

        internal ContextAndServiceConfiguration GetMasterGroupCommConfiguration(string evaluatorId)
        {
            lock (_lock)
            {
                if (_masterFailed || _assignedPartitionDescriptors == 0)
                {
                    _masterFailed = false;
                    _masterEvaluatorId = evaluatorId;
                    return _masterGroupCommConfiguration;
                }
                Exceptions.Throw(
                    new Exception(
                        "Either master did not fail or this is not first requested evaluator. So cannot give master group comm. configuration"),
                    Logger);
            }
            return null;
        }

        /// <summary>
        /// Gives context and service configuration for next evaluator either from failed 
        /// evaluator or new configuration
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns></returns>
        internal ContextAndServiceConfiguration GetNextGroupCommConfiguration(string evaluatorId)
        {
            lock (_lock)
            {
                Console.WriteLine("$$$$$$$Inside next group omm. config");
                if (_dataLoadedEvaluators.Contains(evaluatorId))
                {
                    Exceptions.Throw(new Exception("The task is already submitted to evaluator with group comm configurations"), Logger);
                }

                if (!_submittedEvaluators.Contains(evaluatorId))
                {
                    Exceptions.Throw(
                        new Exception(
                            "The evaluator was never scheduled for data loading, yet it entered Group comm. set up stage"),
                        Logger);
                }

                _submittedEvaluators.Remove(evaluatorId);
                _dataLoadedEvaluators.Add(evaluatorId);
                Console.WriteLine("$$$$$$$Getting group omm. config {0} {1} {2} {3}",
                    _submittedEvaluators.Count,
                    _dataLoadedEvaluators.Count,
                    _partitionDescriptors.Count,
                    _failedEvaluators.Count);
                return _configurationProvider[evaluatorId].GroupCommConfig;
            }
        }

        /// <summary>
        /// Gives context and service configuration for next evaluator either from failed 
        /// evaluator or new configuration
        /// </summary>
        /// <param name="evaluatorId"></param>
        /// <returns></returns>
        internal ContextAndServiceConfiguration GetNextDataLoadingConfiguration(string evaluatorId)
        {
            lock (_lock)
            {
                if (_submittedEvaluators.Contains(evaluatorId))
                {
                    Exceptions.Throw(new Exception("The tas is already running on evaluator"), Logger);
                }

                if (_dataLoadedEvaluators.Contains(evaluatorId))
                {
                    Exceptions.Throw(new Exception("The evaluator already has the data loaded"), Logger);
                }

                if (_failedEvaluators.Count == 0 && _assignedPartitionDescriptors >= _numNodes - 1)
                {
                    Exceptions.Throw(new Exception("No more data configuration can be provided"), Logger);
                }

                // if some failed id exists return that configuration
                if (_failedEvaluators.Count != 0)
                {
                    Console.WriteLine("$$$$$$$failures getting data loading ocnf");
                    string failedEvaluatorId = _failedEvaluators.First();
                    var configurations = _configurationProvider[failedEvaluatorId];
                    _configurationProvider.Remove(failedEvaluatorId);
                    _configurationProvider[evaluatorId] = configurations;
                    _failedEvaluators.Remove(failedEvaluatorId);
                }
                else
                {
                    _assignedPartitionDescriptors++;

                    if (_configurationProvider.ContainsKey(evaluatorId))
                    {
                        Exceptions.Throw(
                            new Exception(
                                "Evaluator Id already present in configuration cache, they have to be unique"),
                            Logger);
                    }
                    Console.WriteLine("$$$$$$$no failures getting data loading conf");

                    _configurationProvider[evaluatorId] = new EvaluatorConfigurations(evaluatorId,
                        GetDataLoadingContextAndServiceConfiguration(_partitionDescriptors.Pop(), evaluatorId),
                        GetMapTaskGroupCommContextAndServiceConfiguration());
                }

                _submittedEvaluators.Add(evaluatorId);
                Console.WriteLine("$$$$$$$Getting group omm. config {0} {1} {2} {3}",
                                   _submittedEvaluators.Count,
                                   _dataLoadedEvaluators.Count,
                                   _partitionDescriptors.Count,
                                   _failedEvaluators.Count);
                
                /*return new ContextAndServiceConfiguration(
                    _configurationProvider[evaluatorId].DataLoadingConfig.Context,
                    Configurations.Merge(_configurationProvider[evaluatorId].GroupCommConfig.Service, _configurationProvider[evaluatorId].DataLoadingConfig.Service));*/
                return _configurationProvider[evaluatorId].DataLoadingConfig;
            }
        }

        private ContextAndServiceConfiguration GetDataLoadingContextAndServiceConfiguration(
            IPartitionDescriptor partitionDescriptor,
            string evaluatorId)
        {
            var dataLoadingContext =
                TangFactory.GetTang()
                    .NewConfigurationBuilder()
                    .BindSetEntry<ContextConfigurationOptions.StartHandlers, DataLoadingContext<TDataHandler>, IObserver<IContextStart>>(GenericType<ContextConfigurationOptions.StartHandlers>.Class,
                            GenericType<DataLoadingContext<TDataHandler>>.Class)
                    .Build();

            var serviceConf =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(ServiceConfiguration.ConfigurationModule.Build(),
                        dataLoadingContext,
                        partitionDescriptor.GetPartitionConfiguration())
                    .Build();

            var contextConf = ContextConfiguration.ConfigurationModule
                .Set(ContextConfiguration.Identifier, string.Format("DataLoading-{0}", evaluatorId))
                .Build();
            return new ContextAndServiceConfiguration(contextConf, serviceConf);
        }

        private ContextAndServiceConfiguration GetMapTaskGroupCommContextAndServiceConfiguration()
        {
            var codecConfig =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(
                        StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Conf.Set(
                            StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Codec,
                            GenericType<MapInputWithControlMessageCodec<TMapInput>>.Class).Build(),
                        StreamingCodecConfigurationMinusMessage<TMapOutput>.Conf.Build(),
                        _configurationManager.MapInputCodecConfiguration)
                    .Build();

            var contextConf = _groupCommDriver.GetContextConfiguration();
            var serviceConf = Configurations.Merge(_groupCommDriver.GetServiceConfiguration(), codecConfig);

            return new ContextAndServiceConfiguration(contextConf, serviceConf);
        }

        private ContextAndServiceConfiguration GetUpdateTaskGroupCommContextAndServiceConfiguration()
        {
            var codecConfig =
                TangFactory.GetTang()
                    .NewConfigurationBuilder(
                        new[]
                        {
                            StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Conf.Set(
                                StreamingCodecConfiguration<MapInputWithControlMessage<TMapInput>>.Codec,
                                GenericType<MapInputWithControlMessageCodec<TMapInput>>.Class).Build(),
                            StreamingCodecConfigurationMinusMessage<TMapOutput>.Conf.Build(),
                            _configurationManager.UpdateFunctionCodecsConfiguration
                        })
                    .Build();

            var serviceConf = Configurations.Merge(_groupCommDriver.GetServiceConfiguration(), codecConfig);
            return new ContextAndServiceConfiguration(_groupCommDriver.GetContextConfiguration(), serviceConf);
        }
    }
}
