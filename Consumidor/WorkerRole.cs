using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Consumidor
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);
        static CloudQueue segundaFila;

        public override void Run()
        {
            Trace.TraceInformation("Consumidor está  sendo executado");
            var connectionString = "DefaultEndpointsProtocol=https;AccountName=buytofun;AccountKey=bLDR0ewVsim9P7JwRp18kPgTDsBXRT3sD+vhgc3bnIHR1yf+xDUSowOkCjuY/jdaJSv4eLsfhAfB3jQnfn9FXg==;EndpointSuffix=core.windows.net";
            CloudStorageAccount cloudStorageAccount;

            if (!CloudStorageAccount.TryParse(connectionString, out cloudStorageAccount))
            {
                Console.WriteLine("Erro na conexão!");
            }

            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();

            segundaFila = cloudQueueClient.GetQueueReference("segundafila");
            segundaFila.CreateIfNotExists();

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            // Definir o número máximo de conexões simultâneas
            ServicePointManager.DefaultConnectionLimit = 12;

            bool result = base.OnStart();

            Trace.TraceInformation("Consumidor foi iniciado");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Consumidor está sendo pausado");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("Consumidor foi parado");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Trabalhando...");

                var cloudQueueMessage = segundaFila.GetMessage();

                if (cloudQueueMessage == null)
                {
                    return;
                }

                segundaFila.DeleteMessage(cloudQueueMessage);

                await Task.Delay(1000);
            }
        }
    }
}
