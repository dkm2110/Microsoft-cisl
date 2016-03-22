using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Org.Apache.REEF.Common.Context;
using Org.Apache.REEF.Common.Events;
using Org.Apache.REEF.IO.PartitionedData;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Utilities.Diagnostics;
using Org.Apache.REEF.Utilities.Logging;

namespace Org.Apache.REEF.IMRU.OnREEF.Driver
{
    internal class DataLoadingContext<T> : IObserver<IContextStart>
    {
        private static readonly Logger Logger = Logger.GetLogger(typeof(DataLoadingContext<T>));

        [Inject]
        internal DataLoadingContext(IInputPartition<T> partition)
        {
            Console.WriteLine("ENTERED CONTEXT");
        }

        public void OnNext(IContextStart value)
        {
        }

        public void OnError(Exception error)
        {
            Exceptions.Throw(error, "Error occured in Data Loading context start", Logger);
        }

        public void OnCompleted()
        {
        }
    }
}
