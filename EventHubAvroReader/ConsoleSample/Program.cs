using EventHubAvroReader;
using System;
using System.Threading.Tasks;

namespace ConsoleSample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // TODO: use something different for arg management.
            var connectionString = args[0];
            var containerName = args[1];
            var fileName = args[2];

            if (String.IsNullOrWhiteSpace(fileName))
            {
                Console.WriteLine("Usage: ConsoleSample.exe {connectionString} {containerName} {fileName}");
            }
            else
            {
                var items = await AvroParser.ParseDataFromCloudStorageAsync(connectionString, containerName, fileName);
                Console.WriteLine("Parsed {0} items", items.Count);
                foreach (var item in items)
                {
                    Console.WriteLine("Enqueued Time: " + item.EnqueuedTimeUtc.ToLongDateString());
                }
            }
        }
    }
}
