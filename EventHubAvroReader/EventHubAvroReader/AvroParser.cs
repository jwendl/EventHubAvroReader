using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace EventHubAvroReader
{
    public static class AvroParser
    {
        public static async Task<List<EventHubAvroData>> ParseDataFromCloudStorageAsync(string connectionString, string containerName, string fileName)
        {
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var containerReference = blobClient.GetContainerReference(containerName);
            var blockBlobReference = containerReference.GetBlockBlobReference(fileName);

            using (var memoryStream = new MemoryStream())
            {
                await blockBlobReference.DownloadToStreamAsync(memoryStream);
                memoryStream.Position = 0;
                return ParseAvroFile(memoryStream);
            }
        }

        public static List<EventHubAvroData> ParseAvroFile(string fileName)
        {
            if (!File.Exists(fileName))
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Error: Missing Avro file " + fileName);
                Console.ResetColor();
            }

            using (var file = File.Open(fileName, FileMode.Open))
            {
                return ParseAvroFile(file);
            }
        }
        public static List<EventHubAvroData> ParseAvroFile(Stream inStream)
        {

            var eventHubAvroItems = new List<EventHubAvroData>();

            using (var reader = Avro.File.DataFileReader<object>.OpenReader(inStream))
            {
                foreach (Avro.Generic.GenericRecord m in reader.NextEntries)
                {
                    var eventHubAvroData = new EventHubAvroData(m);
                    eventHubAvroItems.Add(eventHubAvroData);
                }
            }

            return eventHubAvroItems;
        }
    }
}
