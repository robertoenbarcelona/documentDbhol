
namespace Todo
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using Models;
    using Newtonsoft.Json.Linq;
    using System.Net;

    public static class DocumentDBRepository
    {
        private static string databaseId;
        private static string collectionId;
        private static Database database;
        private static DocumentCollection collection;
        private static DocumentClient client;

        private static string DatabaseId
        {
            get
            {
                if (string.IsNullOrEmpty(databaseId))
                {
                    databaseId = ConfigurationManager.AppSettings["database"];
                }

                return databaseId;
            }
        }

        private static string CollectionId
        {
            get
            {
                if (string.IsNullOrEmpty(collectionId))
                {
                    collectionId = ConfigurationManager.AppSettings["collection"];
                }

                return collectionId;
            }
        }

        private static Database Database
        {
            get
            {
                if (database == null)
                {
                    database = ReadOrCreateDatabase();
                }

                return database;
            }
        }

        private static DocumentCollection Collection
        {
            get
            {
                if (collection == null)
                {
                    collection = ReadOrCreateCollection(Database.SelfLink);
                }

                return collection;
            }
        }

        private static DocumentClient Client
        {
            get
            {
                if (client == null)
                {
                    string endpoint = ConfigurationManager.AppSettings["endpoint"];
                    string authKey = ConfigurationManager.AppSettings["authKey"];
                    Uri endpointUri = new Uri(endpoint);
                    client = new DocumentClient(endpointUri, authKey);
                }

                return client;
            }
        }

        public static List<Item> GetIncompleteItems()
        {
            return Client.CreateDocumentQuery<Item>(Collection.DocumentsLink)
                       .Where(d => !d.Completed)
                       .AsEnumerable()
                       .ToList<Item>();
        }
        
        public static async Task<Document> CreateItemAsync(Item item)
        {
            return await Client.CreateDocumentAsync(Collection.SelfLink, item);
        }

        public static Item GetItem(string id)
        {
            return Client.CreateDocumentQuery<Item>(Collection.DocumentsLink)
                            .Where(d => d.Id == id)
                            .AsEnumerable()
                            .FirstOrDefault();
        }

        public static Document GetDocument(string id)
        {
            return Client.CreateDocumentQuery(Collection.DocumentsLink)
                          .Where(d => d.Id == id)
                          .AsEnumerable()
                          .FirstOrDefault();
        }

        public static async Task<Document> UpdateItemAsync(Item item)
        {
            Document doc = GetDocument(item.Id);
            return await Client.ReplaceDocumentAsync(doc.SelfLink, item);
        }

        public static async Task DeleteItemAsync(string id)
        {
            Document doc = GetDocument(id);
            await Client.DeleteDocumentAsync(doc.SelfLink);
        }

        public static async Task<Item> UpdateItemConcurrencyAsync(Item item)
        {
            dynamic json = JObject.FromObject(item);
            json.TimeStamp = "fake";

            // Using Access Conditions gives us the ability to use the ETag from our fetched document for optimistic concurrency.
            var ac = new AccessCondition { Condition = json.TimeStamp, Type = AccessConditionType.IfMatch };
            try
            {

                Document document = await Client.ReplaceDocumentAsync(
                            UriFactory.CreateDocumentUri(DatabaseId, CollectionId, item.Id),
                            json,
                            new RequestOptions { AccessCondition = ac })
                            .ConfigureAwait(false);

                var entity = (Item)(dynamic)document;
                entity.Description = document.ETag;
                return entity;
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode == HttpStatusCode.PreconditionFailed)
                { throw new Exception($"Updating entity with {item.Id} result in conflict"); }

                throw;
            }
        }

        private static DocumentCollection ReadOrCreateCollection(string databaseLink)
        {
            var col = Client.CreateDocumentCollectionQuery(databaseLink)
                                    .Where(c => c.Id == CollectionId)
                                    .AsEnumerable()
                                    .FirstOrDefault();

            if (col == null)
            {
                col = Client.CreateDocumentCollectionAsync(databaseLink, new DocumentCollection { Id = CollectionId }).Result;
            }

            return col;
        }

        private static Database ReadOrCreateDatabase()
        {
            var db = Client.CreateDatabaseQuery()
                                 .Where(d => d.Id == DatabaseId)
                                 .AsEnumerable()
                                 .FirstOrDefault();

            if (db == null)
            {
                db = Client.CreateDatabaseAsync(new Database { Id = DatabaseId }).Result;
            }

            return db;
        }
    }
}