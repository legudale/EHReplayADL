using Microsoft.Azure.DataLake.Store;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest.Azure.Authentication;
using System;
using System.Collections.Generic;
using System.IO;

namespace EHReplayADL
{
    public interface IArchiveItemEnumerator
    {
        IEnumerable<ArchiveItem> GetItems();
    }

    public class RootArchiveItemEnumerator : IArchiveItemEnumerator
    {
        public RootArchiveItemEnumerator(ExecutionContext ctx, AdlsClient client, string root)
        {
            Ctx = ctx;
            Client = client;
            Root = root;
        }


        public ExecutionContext Ctx { get; }
        public AdlsClient Client { get; }
        public string Root { get; }


        public IEnumerable<ArchiveItem> GetItems()
        {
            var stack = new Stack<string>();
            stack.Push(Root);
            while (stack.Count > 0)
            {
                var path = stack.Pop();
                var subdirs = new List<string>();
                foreach (var entry in Client.EnumerateDirectory(path))
                    if (entry.Type == DirectoryEntryType.DIRECTORY)
                    {
                        if (entry.Name.EndsWith("temp", StringComparison.Ordinal))
                        {
                            if (Ctx.Noisy) Console.WriteLine($"Skipping {entry.FullName}");
                        }
                        else
                        {
                            subdirs.Add(entry.FullName);
                        }
                    }
                    else
                    {
                        ADLArchiveItem archiveItem = null;
                        try
                        {
                            archiveItem = new ADLArchiveItem(Client, entry);
                        }
                        catch (ApplicationException)
                        {
                            if (Ctx.Noisy)
                                Console.WriteLine($"Skipping item with unexpected item name {entry.FullName}");
                        }

                        if (archiveItem != null)
                            yield return archiveItem;
                    }

                subdirs.Sort();
                subdirs.Reverse();
                foreach (var subdir in subdirs) stack.Push(subdir);
            }
        }
    }

    public class ListArchiveItemEnumerator : IArchiveItemEnumerator
    {
        public ListArchiveItemEnumerator(ExecutionContext ctx, AdlsClient client, string listFileName)
        {
            Ctx = ctx;
            Client = client;
            ListFileName = listFileName;
        }

        public ExecutionContext Ctx { get; }
        public AdlsClient Client { get; }
        public string ListFileName { get; }

        public IEnumerable<ArchiveItem> GetItems()
        {
            foreach (var line in File.ReadAllLines(ListFileName))
            {
                var entry = Client.GetDirectoryEntry(line);
                yield return new ADLArchiveItem(Client, entry);
            }
        }
    }


    public class AdlArchive : IEventHubArchive
    {
        public AdlsClient Client { get; private set; }
        public string Root { get; private set; }
        public string AdlList { get; private set; }
        internal ExecutionContext Ctx { get; private set; }

        public IEnumerable<ArchiveItem> GetItems()
        {
            if (AdlList != null)
            {
                if (Ctx.Noisy) Console.WriteLine($"Enumerating based on the list stored in {AdlList}");
                var itemEnumerator = new ListArchiveItemEnumerator(Ctx, Client, AdlList);
                return itemEnumerator.GetItems();
            }
            else
            {
                if (Ctx.Noisy) Console.WriteLine($"Enumerating starting with the root {Root}");
                var itemEnumerator = new RootArchiveItemEnumerator(Ctx, Client, Root);
                return itemEnumerator.GetItems();
            }
        }

        internal static AdlArchive FromConfig(ExecutionContext ctx, IConfigurationRoot config)
        {
            var clientId = config["adlClientId"];
            var clientSecret = config["adlClientSecret"];
            var tenantId = config["adlTenantId"];
            var path = config["adlPath"];

            var creds = new ClientCredential(clientId, clientSecret);
            var token = ApplicationTokenProvider.LoginSilentAsync(tenantId, creds).Result;
            var archive = new AdlArchive();
            archive.Client = AdlsClient.CreateClient(path, token);
            archive.Ctx = ctx;
            archive.Root = config["adlRoot"];
            archive.AdlList = config["adlList"];


            return archive;
        }
    }
}