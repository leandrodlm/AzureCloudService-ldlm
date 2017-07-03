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

namespace Produtor
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        static CloudQueue primeiraFila;
        static CloudQueue segundaFila;

        public override void Run()
        {
            Trace.TraceInformation("Produtor está sendo executado");
            var connectionString = "DefaultEndpointsProtocol=https;AccountName=buytofun;AccountKey=/wxy9kxlnPRHIc9wpRSRJWx4ZHmXgvt2KsWdBSPCNyDuh2MMIf73T0zlsjPmMw9aKe9z8qCvvbvTFpcHygoFyg==;EndpointSuffix=core.windows.net";
            CloudStorageAccount cloudStorageAccount;

            if (!CloudStorageAccount.TryParse(connectionString, out cloudStorageAccount))
            {
                Console.WriteLine("Erro na conexão!");
            }

            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();

            primeiraFila = cloudQueueClient.GetQueueReference("primeirafila");
            primeiraFila.CreateIfNotExists();

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

            Trace.TraceInformation("Produtor foi iniciado");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Produtor está sendo pausado");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("Produtor foi parado");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Produtor em execução");

                var connectionString = "";

                CloudStorageAccount cloudStorageAccount;

                if (!CloudStorageAccount.TryParse(connectionString, out cloudStorageAccount))
                {
                    Console.WriteLine("Erro na conexão!");
                }
                var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
                var primeiraFila = cloudQueueClient.GetQueueReference("primeiraqueue");
                primeiraFila.CreateIfNotExists();

                var segundaFila = cloudQueueClient.GetQueueReference("segundaqueue");
                segundaFila.CreateIfNotExists();

                var cloudQueueMessage = primeiraFila.GetMessage();

                if (cloudQueueMessage == null)
                {
                    return;
                }
                else
                {
                    primeiraFila.DeleteMessage(cloudQueueMessage);
                    var message = new CloudQueueMessage("Mensagem");
                    segundaFila.AddMessage(message);
                }

                // frequência de 5 segundos
                await Task.Delay(5000);
            }
        }
    }
}
