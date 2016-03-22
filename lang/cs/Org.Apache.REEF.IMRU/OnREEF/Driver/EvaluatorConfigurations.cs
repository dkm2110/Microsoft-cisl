using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.Common.Evaluator;
using Org.Apache.REEF.Tang.Interface;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    internal class EvaluatorConfigurations
    {
        private readonly string _id;
        private readonly ContextAndServiceConfiguration _dataLoadingConfiguration;
        private readonly ContextAndServiceConfiguration _groupCommunicationConfiguration;

        internal EvaluatorConfigurations(string id,
            ContextAndServiceConfiguration dataLoadingConfig,
            ContextAndServiceConfiguration groupCommConfig)
        {
            _id = id;
            _dataLoadingConfiguration = dataLoadingConfig;
            _groupCommunicationConfiguration = groupCommConfig;
        }

        internal string Id
        {
            get { return _id; }
        }

        internal ContextAndServiceConfiguration DataLoadingConfig
        {
            get
            {
                return _dataLoadingConfiguration;
            }
        }

        internal ContextAndServiceConfiguration GroupCommConfig
        {
            get
            {
                return _groupCommunicationConfiguration; 
            }
        }
    }
}
