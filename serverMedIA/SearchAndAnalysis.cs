using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data.SqlClient;
using System.Data;

using System.Net;
using System.IO;

using Microsoft.Azure.CognitiveServices.Language.TextAnalytics;
using Microsoft.Azure.CognitiveServices.Language.TextAnalytics.Models;

using Newtonsoft.Json.Linq;
using System.Text.RegularExpressions;
using System.Configuration;

using Model;

namespace serverMedIA
{
    public static class SearchAndAnalysis
    {
        public static string API_SEARCH_KEY = ConfigurationManager.ConnectionStrings["API_SEARCH_KEY"].ConnectionString;
        public static string URI_API_SEARCH_KEY = ConfigurationManager.ConnectionStrings["URI_API_SEARCH_KEY"].ConnectionString;
        public static int MAX_NUMBER_NEWS = Int32.Parse(ConfigurationManager.ConnectionStrings["MAX_NUMBER_NEWS"].ConnectionString);
        public static string SQLDB_CONNECTION = ConfigurationManager.ConnectionStrings["SQLDB_CONNECTION"].ConnectionString;
        public static string API_TEXT_ANALITICS_KEY = ConfigurationManager.ConnectionStrings["API_TEXT_ANALITICS_KEY"].ConnectionString;
        public static string URI_API_TEXT_ANALITICS = ConfigurationManager.ConnectionStrings["URI_API_TEXT_ANALITICS"].ConnectionString;
        public static int MAX_NUMBER_OPINION = Int32.Parse(ConfigurationManager.ConnectionStrings["MAX_NUMBER_OPINION"].ConnectionString);
        public static string LANGUAGE_TEXT_ANALITICS = ConfigurationManager.ConnectionStrings["LANGUAGE_TEXT_ANALITICS"].ConnectionString;
        public static int MAX_NUMBER_KEY_PHRASE = Int32.Parse(ConfigurationManager.ConnectionStrings["MAX_NUMBER_KEY_PHRASE"].ConnectionString);
        public static string URI_API_VIDEOSEARCH_KEY = ConfigurationManager.ConnectionStrings["URI_API_VIDEOSEARCH_KEY"].ConnectionString;
        public static int MAX_NUMBER_VIDEOS = Int32.Parse(ConfigurationManager.ConnectionStrings["MAX_NUMBER_VIDEOS"].ConnectionString);

        public static SqlConnection connection;
        public static SqlTransaction transaction;
        public static TraceWriter log;

        [FunctionName("SearchAndAnalysis")]
        public static void Run([TimerTrigger("0 0 0/2 * * *")]TimerInfo myTimer, TraceWriter _log)
        {
            log = _log;

            log.Info("function app working >>");

            using (DB_medIAEntities db = new DB_medIAEntities())
            {
                News news = new News();
                Mention mention = new Mention();
                News_Mention nm = new News_Mention();

                nm.Mention = mention;
                nm.News = news;

                news.News_Mention.Add(nm);
                

                
            }
            
            

            //SearchNews();

            //ExtractOpinionsFromNews();

            //ExtractKeyPhrasesFromNews();

            //SearchVideos();
        }

        #region SQL objects

        public static bool OpenConection()
        {
            try
            {
                connection = new SqlConnection(SQLDB_CONNECTION);
                connection.Open();
            }
            catch (Exception e)
            {
                log.Error("Database connection failed: " + e.Message);
                return false;
            }
            return true;
        }

        public static bool CloseConnection()
        {
            try
            {
                connection.Close();
            }
            catch (Exception e)
            {
                log.Error("Database DISconnect failed: " + e.Message);
                return false;
            }
            return true;
        }

        public static SqlCommand Query(string Query_)
        {
            SqlCommand cmd = new SqlCommand(Query_, connection, transaction);
            return cmd;
        }

        public static DataTable GetDataTable(SqlCommand comando)
        {
            DataTable dt = new DataTable();
            SqlDataAdapter datos = new SqlDataAdapter(comando);
            datos.Fill(dt);
            return dt;
        }

        public static int ExecuteQuery(string Query_)
        {
            SqlCommand cmd = new SqlCommand(Query_, connection, transaction);
            return cmd.ExecuteNonQuery();
        }

        #endregion

        #region News

        public static void SearchNews()
        {
            log.Info("Opening conexion for News Search...");
            if (!OpenConection())
            {
                return;
            }

            JObject json;
            int? idCategory = null, idNews = null;
            int idTerm;
            string term;

            DataTable termsToSearch = GetTermsToSearch();

            if (termsToSearch != null)
            {
                foreach (DataRow row in termsToSearch.Rows)
                {
                    idTerm = Int32.Parse(row.ItemArray[0].ToString());
                    term = row.ItemArray[1].ToString();

                    log.Info("\n\n>>>>>>>>>>>>>>>>>>>>>Searching news for '" + term + "' <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

                    json = JObject.Parse(BingNewsSearch(term));

                    foreach (var value in json["value"])
                    {
                        transaction = connection.BeginTransaction("Check News");

                        idNews = IsNewsInDB(value["url"].ToString());

                        if (idNews != null)
                        {
                            if (InsertNewsTermTosearchInDB(idNews.Value, idTerm))
                            {
                                transaction.Commit();
                                continue;
                            }
                            else
                            {
                                transaction.Rollback();
                                continue;
                            }
                        }
                        else
                        {
                            transaction.Commit();
                        }


                        if (idNews == null)
                        {
                            transaction = connection.BeginTransaction("Insert News");

                            if (InsertCategory(value["category"], ref idCategory) &&
                                InsertNewsInDB(value, idCategory, ref idNews) &&
                                InsertNewsTermTosearchInDB(idNews.Value, idTerm) &&
                                InsertPublishers(value["provider"], idNews.Value) &&
                                InsertImage(value["image"], idNews.Value) &&
                                InsertAbouts(value["about"], idNews.Value) &&
                                InsertMentions(value["mentions"], idNews.Value))
                            {
                                log.Info("News with all information inserted successfully: " + idNews);
                                transaction.Commit();
                            }
                            else
                            {
                                transaction.Rollback();
                                continue;
                            }
                        }
                    }
                }
            }

            CloseConnection();
        }

        public static DataTable GetTermsToSearch()
        {
            DataTable termsToSearch = null;

            try
            {
                termsToSearch = GetDataTable(Query("SELECT id, term FROM TermToSearch"));
            }
            catch (Exception e)
            {
                log.Info("Error at getting terms to search : \n" + e.Message);
            }
            return termsToSearch;
        }

        public static string[] GetDifferentNews(JObject json)
        {
            if (json["value"].Count() == 0)
            {
                return null;
            }

            string query = @"SELECT url FROM (values @values) as T(url)
                            EXCEPT
                            SELECT url FROM News.News;";

            StringBuilder sb = new StringBuilder();
            foreach (var value in json["value"])
            {
                sb.Append("('");
                sb.Append(value["url"]);
                sb.Append("'),");
            }
            sb.Remove(sb.Length - 1, 1);

            query = query.Replace("@values", sb.ToString());

            try
            {
                DataTable result = GetDataTable(Query(query));
                string[] differentNews = new string[result.Rows.Count];

                log.Info("About to insert following news: \n");
                int resultRowsCount = result.Rows.Count;
                for (int i = 0; i < resultRowsCount; i++)
                {
                    differentNews[i] = result.Rows[i].ItemArray[0].ToString();
                    // log.Info(differentNews[i]);
                }


                return differentNews;
            }
            catch (Exception e)
            {
                log.Error("Error at query: \n" + query + "\n" + e.Message);
                return null;
            }
        }

        public static int? IsNewsInDB(string url)
        {
            int? idNews = null;
            string query = @"SELECT id from News.News WHERE url = '" + url + "'";

            try
            {
                DataTable result = GetDataTable(Query(query));

                if (result.Rows.Count > 0)
                {
                    log.Info("News already in DB: " + url);

                    idNews = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                    return idNews;
                }
                else
                {
                    log.Info("News does not exist in DB: " + url);
                }
            }
            catch (Exception e)
            {
                log.Error("Error at searching url: \n" + query + "\n" + e.Message);
            }

            return idNews;
        }

        public static int? IsNewsTermToSearchInDB(int idNews, int idTermToSearch)
        {
            int? idNewsTermToSearch = null;
            string query = @"SELECT id from News.News_TermToSearch WHERE idNews = " + idNews + " AND idTermToSearch = " + idTermToSearch;

            try
            {
                DataTable result = GetDataTable(Query(query));

                if (result.Rows.Count > 0)
                {
                    idNewsTermToSearch = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                    log.Info("News and Term already related in DB: " + idNewsTermToSearch);
                }
            }
            catch (Exception e)
            {
                log.Error("Error at searching News_TermToSearch: \n" + query + "\n" + e.Message);
            }

            return idNewsTermToSearch;
        }

        public static bool InsertNewsInDB(JToken value, int? idCategory, ref int? idNews)
        {
            string datePublished = DateTime.Parse(value["datePublished"].ToString()).ToString("yyyy-MM-dd hh:mm:ss");
            string name = value["name"].ToString().Replace("'", "");
            string url = value["url"].ToString();
            string description = value["description"].ToString().Replace("'", "");

            string query = @"INSERT INTO News.News (idCategory,datePublished,name,url,description) VALUES 
                             (@idCategory, '@datePublished', '@name', '@url', '@description')
                             SELECT @@IDENTITY AS 'ID'";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@idCategory", idCategory.HasValue ? "'" + idCategory.Value + "'" : "NULL");
            sb.Replace("@datePublished", datePublished);
            sb.Replace("@name", name);
            sb.Replace("@url", url);
            sb.Replace("@description", description);

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                idNews = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                log.Info("Succeded inserting News: " + idNews);

                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at inserting News: \n" + sb.ToString() + "\n" + e.Message);

                idNews = null;
                return false;
            }
        }

        public static bool InsertNewsTermTosearchInDB(int idNews, int idTerm)
        {
            string query = @"IF NOT EXISTS(SELECT id FROM News.News_TermToSearch WHERE idNews = @idNews AND idTermToSearch = @idTerm)
                            BEGIN
                            INSERT INTO [News].[News_TermToSearch] (idNews, idTermToSearch)
                            VALUES ('@idNews','@idTerm') 
                            END";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@idNews", idNews.ToString());
            sb.Replace("@idTerm", idTerm.ToString());

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));

                log.Info("Succeded inserting News_TermToSearch");
                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at inserting News_TermToSearch: \n" + sb.ToString() + "\n" + e.Message);
                return false;
            }
        }

        #region Category       

        public static bool InsertCategory(JToken category, ref int? idCategory)
        {
            if (category == null)
            {
                return true;
            }

            string name = category.ToString();
            if (string.IsNullOrEmpty(name))
            {
                return true;
            }

            idCategory = IsCategoryInDB(name);

            if (idCategory == null)
            {
                idCategory = InsertCategoryInDB(name);
            }

            return idCategory.HasValue;
        }

        public static int? IsCategoryInDB(string name)
        {
            int? idCategory = null;
            string query = @"SELECT id from News.Category WHERE name = '" + name + "'";

            try
            {
                DataTable result = GetDataTable(Query(query));

                if (result.Rows.Count > 0)
                {
                    log.Info("Category already in DB: " + name);
                    idCategory = Int32.Parse(result.Rows[0].ItemArray[0].ToString());
                }
            }
            catch (Exception e)
            {
                log.Error("Error at searching category: \n" + query + "\n" + e.Message);
            }

            return idCategory;
        }

        public static int? InsertCategoryInDB(string name)
        {
            int? idCategory = null;
            string query = @"INSERT INTO News.Category (name) VALUES ('@name') 
                             SELECT @@IDENTITY AS 'ID' ";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@name", name);

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                idCategory = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                log.Info("Succeded inserting Category: " + idCategory);
            }
            catch (Exception e)
            {
                log.Error("Error at inserting Category: \n" + sb.ToString() + "\n" + e.Message);
            }

            return idCategory;
        }

        #endregion

        #region About

        public static bool InsertAbouts(JToken about, int idNews)
        {
            if (about == null)
            {
                return true;
            }

            List<int> ids = new List<int>(5);
            int? idAbout;

            foreach (var a in about)
            {
                idAbout = IsAboutInDB(a["readLink"].ToString());

                if (idAbout != null)
                {
                    ids.Add(idAbout.Value);
                }
                else
                {
                    idAbout = InsertAboutInDB(a["readLink"].ToString(), a["name"].ToString());

                    if (idAbout != null)
                    {
                        ids.Add(idAbout.Value);
                    }
                }
            }

            return InsertNewsAboutInDB(ids, idNews);
        }

        public static int? IsAboutInDB(string readLink)
        {
            int? idAbout = null;
            string query = @"SELECT id from News.About WHERE readLink = '" + readLink + "'";

            try
            {
                DataTable result = GetDataTable(Query(query));

                if (result.Rows.Count > 0)
                {
                    log.Info("About already in DB: " + readLink);
                    idAbout = Int32.Parse(result.Rows[0].ItemArray[0].ToString());
                }
            }
            catch (Exception e)
            {
                log.Error("Error at searching about: \n" + query + "\n" + e.Message);
            }

            return idAbout;
        }

        public static int? InsertAboutInDB(string readLink, string name)
        {
            int? idAbout = null;
            string query = @"INSERT INTO News.About (readLink,name) VALUES ('@readLink', '@name') 
                             SELECT @@IDENTITY AS 'ID' ";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@readLink", readLink)
              .Replace("@name", name.Replace("'", ""));

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                idAbout = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                log.Info("Succeded inserting About: " + idAbout);
            }
            catch (Exception e)
            {
                log.Error("Error at inserting About: \n" + sb.ToString() + "\n" + e.Message);
            }

            return idAbout;
        }

        public static bool InsertNewsAboutInDB(List<int> idsAbout, int idNews)
        {
            if (idsAbout == null || idsAbout.Count == 0)
            {
                return true;
            }

            string query = @"INSERT INTO News.News_About (idNews, idAbout) VALUES @values";

            StringBuilder sb = new StringBuilder();
            foreach (int idAbout in idsAbout)
            {
                sb.Append("('");
                sb.Append(idNews);
                sb.Append("', '");
                sb.Append(idAbout);
                sb.Append("'),");
            }
            sb.Remove(sb.Length - 1, 1);

            query = query.Replace("@values", sb.ToString());

            try
            {
                ExecuteQuery(query);

                log.Info("Succeded inserting News_About");
                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at inserting News_About: \n" + sb.ToString() + "\n" + e.Message);
                return false;
            }
        }

        #endregion

        #region Publisher

        public static bool InsertPublishers(JToken publisher, int idNews)
        {
            if (publisher == null)
            {
                return false;
            }

            List<int> ids = new List<int>(5);
            int? idPublisher, idPublisherType;

            foreach (var a in publisher)
            {
                idPublisher = IsPublisherInDB(a["name"].ToString());

                if (idPublisher != null)
                {
                    ids.Add(idPublisher.Value);
                }
                else
                {
                    idPublisherType = InsertPublisherTypeInDB(a["_type"].ToString());
                    if (idPublisherType != null)
                    {
                        idPublisher = InsertPublisherInDB(a["name"].ToString(), idPublisherType.Value);

                        if (idPublisher != null)
                        {
                            ids.Add(idPublisher.Value);
                        }
                    }
                }
            }

            return InsertNewsPublisherInDB(ids, idNews);
        }

        public static int? IsPublisherInDB(string name)
        {
            int? idPublisher = null;
            string query = @"SELECT id from dbo.Publisher WHERE name = '" + name + "'";

            try
            {
                DataTable result = GetDataTable(Query(query));

                if (result.Rows.Count > 0)
                {
                    log.Info("Publisher already in DB: " + name);
                    idPublisher = Int32.Parse(result.Rows[0].ItemArray[0].ToString());
                }
            }
            catch (Exception e)
            {
                log.Error("Error at searching publisher: \n" + query + "\n" + e.Message);
            }

            return idPublisher;
        }

        public static int? InsertPublisherTypeInDB(string _type)
        {
            int? idPublisherType = null;
            string query = @"INSERT INTO dbo.PublisherType (name) VALUES ('@_type') 
                             SELECT @@IDENTITY AS 'ID' ";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@_type", _type);

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                idPublisherType = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                log.Info("Succeded inserting PublisherType: " + idPublisherType);
            }
            catch (Exception e)
            {
                log.Error("Error at inserting PublisherType: \n" + sb.ToString() + "\n" + e.Message);
            }

            return idPublisherType;
        }

        public static int? InsertPublisherInDB(string name, int idPublisherType)
        {
            int? idPublisher = null;
            string query = @"INSERT INTO dbo.Publisher (name, idType) VALUES ('@name', '@idType') 
                             SELECT @@IDENTITY AS 'ID' ";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@name", name);
            sb.Replace("@idType", idPublisherType.ToString());

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                idPublisher = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                log.Info("Succeded inserting Publisher: " + idPublisher);
            }
            catch (Exception e)
            {
                log.Error("Error at inserting Publisher: \n" + sb.ToString() + "\n" + e.Message);
            }

            return idPublisher;
        }

        public static bool InsertNewsPublisherInDB(List<int> idsPublisher, int idNews)
        {
            if (idsPublisher == null || idsPublisher.Count == 0)
            {
                return false;
            }

            string query = @"INSERT INTO News.News_Publisher (idNews, idPublisher) VALUES @values";

            StringBuilder sb = new StringBuilder();
            foreach (int idPublisher in idsPublisher)
            {
                sb.Append("('");
                sb.Append(idNews);
                sb.Append("', '");
                sb.Append(idPublisher);
                sb.Append("'),");
            }
            sb.Remove(sb.Length - 1, 1);

            query = query.Replace("@values", sb.ToString());

            try
            {
                ExecuteQuery(query);

                log.Info("Succeded inserting News_Publisher");
                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at inserting News_Publisher: \n" + sb.ToString() + "\n" + e.Message);
                return false;
            }
        }

        #endregion

        #region Mentions

        public static bool InsertMentions(JToken mention, int idNews)
        {
            if (mention == null)
            {
                return true;
            }

            List<int> ids = new List<int>(5);
            int? idMention;

            foreach (var a in mention)
            {
                idMention = IsMentionInDB(a["name"].ToString());

                if (idMention != null)
                {
                    ids.Add(idMention.Value);
                }
                else
                {
                    idMention = InsertMentionInDB(a["name"].ToString());

                    if (idMention != null)
                    {
                        ids.Add(idMention.Value);
                    }
                }
            }

            return InsertNewsMentionInDB(ids, idNews);
        }

        public static int? IsMentionInDB(string name)
        {
            int? idMention = null;
            string query = @"SELECT id from News.Mention WHERE name = '" + name + "'";

            try
            {
                DataTable result = GetDataTable(Query(query));

                if (result.Rows.Count > 0)
                {
                    log.Info("Mention already in DB: " + name);
                    idMention = Int32.Parse(result.Rows[0].ItemArray[0].ToString());
                }
                else
                {
                    log.Info("Inserting mention in DB: " + name);
                }
            }
            catch (Exception e)
            {
                log.Error("Error at searching mention: \n" + query + "\n" + e.Message);
            }

            return idMention;
        }

        public static int? InsertMentionInDB(string name)
        {
            int? idMention = null;
            string query = @"INSERT INTO News.Mention (name) VALUES ('@name') 
                             SELECT @@IDENTITY AS 'ID' ";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@name", name);

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                idMention = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                log.Info("Succeded inserting Mention: " + idMention);
            }
            catch (Exception e)
            {
                log.Error("Error at inserting Mention: \n" + sb.ToString() + "\n" + e.Message);
            }

            return idMention;
        }

        public static bool InsertNewsMentionInDB(List<int> idsMention, int idNews)
        {
            if (idsMention == null || idsMention.Count == 0)
            {
                return true;
            }

            string query = @"INSERT INTO News.News_Mention (idNews, idMention) VALUES @values";

            StringBuilder sb = new StringBuilder();
            foreach (int idMention in idsMention)
            {
                sb.Append("(");
                sb.Append(idNews);
                sb.Append(", ");
                sb.Append(idMention);
                sb.Append("),");
            }
            sb.Remove(sb.Length - 1, 1);

            query = query.Replace("@values", sb.ToString());

            try
            {
                ExecuteQuery(query);

                log.Info("Succeded inserting News_Mention");
                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at inserting News_Mention: \n" + query + "\n" + e.Message);
                return false;
            }
        }

        #endregion

        #region Image       

        public static bool InsertImage(JToken image, int idNews)
        {
            if (image == null)
            {
                return true;
            }

            string contentUrl = image["contentUrl"]?.ToString();
            string thumbnailContentUrl = image["thumbnail"]?["contentUrl"].ToString();
            string thumbnailWidth = image["thumbnail"]?["width"].ToString();
            string thumbnailHeight = image["thumbnail"]?["height"].ToString();

            int? idImage = InsertImageInDB(contentUrl, thumbnailContentUrl, thumbnailWidth, thumbnailHeight, idNews);

            return idImage.HasValue;
        }

        public static int? InsertImageInDB(string contentUrl, string thumbnailContentUrl, string thumbnailWidth, string thumbnailHeight, int idNews)
        {
            int? idImage = null;
            string query = @"INSERT INTO [News].[Image] (contentUrl,thumbnailContentUrl,thumbnailWidth,thumbnailHeight,idNews)
                             VALUES (@contentUrl,'@thumbnailContentUrl','@thumbnailWidth','@thumbnailHeight', '@idNews') 
                             SELECT @@IDENTITY AS 'ID' ";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@contentUrl", contentUrl == null ? "NULL" : "'" + contentUrl + "'");
            sb.Replace("@thumbnailContentUrl", thumbnailContentUrl);
            sb.Replace("@thumbnailWidth", thumbnailWidth);
            sb.Replace("@thumbnailHeight", thumbnailHeight);
            sb.Replace("@idNews", idNews.ToString());

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                idImage = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                log.Info("Succeded inserting Image: " + idImage);
            }
            catch (Exception e)
            {
                log.Error("Error at inserting Image: \n" + sb.ToString() + "\n" + e.Message);
            }

            return idImage;
        }

        #endregion

        #region Bing API Search

        public static string BingNewsSearch(string searchQuery)
        {
            // Construct the URI of the search request
            var uriQuery = URI_API_SEARCH_KEY + "?q=" + Uri.EscapeDataString(searchQuery) + "&count=" + MAX_NUMBER_NEWS + "&mkt=es-MX" + "&freshness=Day" + "&sortBy=Date";

            // Perform the Web request and get the response
            WebRequest request = HttpWebRequest.Create(uriQuery);
            request.Headers["Ocp-Apim-Subscription-Key"] = API_SEARCH_KEY;
            HttpWebResponse response = (HttpWebResponse)request.GetResponseAsync().Result;
            string json = new StreamReader(response.GetResponseStream()).ReadToEnd();

            return json;
        }

        public static string BingVideoSearch(string searchQuery)
        {
            // Construct the URI of the search request
            var uriQuery = URI_API_VIDEOSEARCH_KEY + "?q=" + Uri.EscapeDataString(searchQuery) + "&count=" + MAX_NUMBER_VIDEOS + "&mkt=es-MX";

            // Perform the Web request and get the response
            WebRequest request = HttpWebRequest.Create(uriQuery);
            request.Headers["Ocp-Apim-Subscription-Key"] = API_SEARCH_KEY;
            HttpWebResponse response = (HttpWebResponse)request.GetResponseAsync().Result;
            string json = new StreamReader(response.GetResponseStream()).ReadToEnd();

            return json;
        }

        #endregion

        #endregion

        #region Text Analitics

        #region Opinions

        public static void ExtractOpinionsFromNews()
        {
            log.Info("Opening conexion for text analitics ...");
            if (!OpenConection())
            {
                return;
            }

            List<MultiLanguageInput> documents = BuildOpinionRequest();

            if (documents.Count > 0)
            {
                ITextAnalyticsAPI client = new TextAnalyticsAPI();
                client.AzureRegion = AzureRegions.Westus;
                client.SubscriptionKey = API_TEXT_ANALITICS_KEY;

                SentimentBatchResult result = client.Sentiment(new MultiLanguageBatchInput(documents));

                int n = 0;
                foreach (var document in result.Documents)
                {
                    if (document.Score.HasValue)
                    {
                        transaction = connection.BeginTransaction("Inserting opinions");

                        if (InsertOpinionInDB(int.Parse(document.Id), document.Score.Value))
                        {
                            transaction.Commit();
                        }
                        else
                        {
                            transaction.Rollback();
                        }
                    }

                    n++;
                    if (((n % (MAX_NUMBER_OPINION / 10)) == 0) || n == MAX_NUMBER_OPINION)
                    {
                        log.Info(String.Format("Succeed at processing opinions {0:0.00} %", (n * 100 / MAX_NUMBER_OPINION)));
                    }

                }

                GageOpinions();
            }

            CloseConnection();
        }

        public static List<MultiLanguageInput> BuildOpinionRequest()
        {
            List<MultiLanguageInput> documents = new List<MultiLanguageInput>(MAX_NUMBER_OPINION);

            StringBuilder queryDescNews = new StringBuilder(@"IF @MAX_NUMBER_OPINION <= (SELECT COUNT(id) FROM News.News WHERE idOpinion IS NULL) 
                                                                SELECT TOP @MAX_NUMBER_OPINION id, description FROM News.News WHERE idOpinion IS NULL");
            queryDescNews.Replace("@MAX_NUMBER_OPINION", MAX_NUMBER_OPINION.ToString());

            try
            {
                DataTable NewsDescriptions = GetDataTable(Query(queryDescNews.ToString()));

                string idNews, text;

                foreach (DataRow row in NewsDescriptions.Rows)
                {
                    idNews = row.ItemArray[0].ToString();
                    text = row.ItemArray[1].ToString();

                    documents.Add(new MultiLanguageInput(LANGUAGE_TEXT_ANALITICS, idNews, text));
                }

                log.Info("Extracting for opinions succeed");
            }
            catch (Exception e)
            {
                log.Error("Error at searching news for opinions extraction: \n" + queryDescNews + "\n" + e.Message);
            }

            return documents;
        }

        public static bool InsertOpinionInDB(int idNews, double score)
        {
            StringBuilder queryInsertOpinion = new StringBuilder(@"INSERT INTO dbo.Opinion (score) VALUES (@score)
                                                                   UPDATE News.News
                                                                   SET idOpinion = (SELECT @@IDENTITY AS 'idOpinion')
                                                                   WHERE id = @idNews ");

            queryInsertOpinion.Replace("@score", score.ToString());
            queryInsertOpinion.Replace("@idNews", idNews.ToString());

            try
            {
                DataTable NewsDescriptions = GetDataTable(Query(queryInsertOpinion.ToString()));

                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at inserting opinions: \n" + queryInsertOpinion + "\n" + e.Message);
                return false;
            }
        }

        public static void GageOpinions()
        {
            OpenConection();

            List<int> frecAcumulada = GetCummulativeFrec();
            int total = frecAcumulada.Last();

            double firstTercentil = total / 3.0;
            double secondTercentil = total / 3.0 * 2.0;
            double thirdTercentil = total;

            double? firstLimit = null, secondLimit = null, thirdLimit = null;

            for (int i = 0; i < 10; i++)
            {
                if (firstLimit == null && frecAcumulada[i] > firstTercentil)
                {
                    firstLimit = (i + 1) * 0.1;
                }
                if (secondLimit == null && frecAcumulada[i] > secondTercentil)
                {
                    secondLimit = (i + 1) * 0.1;
                }
            }
            thirdLimit = 1;

            InsertOpinionLimitsInDB(firstLimit.Value, secondLimit.Value, thirdLimit.Value);

            CloseConnection();
        }

        public static List<int> GetCummulativeFrec()
        {
            string histogramaOpiniones = @"SELECT * FROM vw_HistogramOpinions ORDER BY clase";

            DataTable frecs = null;
            List<int> frecAcumulada = new List<int>(10);

            try
            {
                frecs = GetDataTable(Query(histogramaOpiniones));

                int className = 0, frec = 0;
                int frecsRowsCount = frecs.Rows.Count;

                if (frecsRowsCount > 0)
                {
                    for (int i = 0, row = 0; i < 10; i++)
                    {
                        if (row < frecsRowsCount)
                        {
                            className = int.Parse(frecs.Rows[row].ItemArray[0].ToString());
                            frec = int.Parse(frecs.Rows[row].ItemArray[1].ToString());
                        }

                        if (className == i + 1)
                        {
                            frecAcumulada.Add(frec + (row == 0 ? 0 : frecAcumulada[i - 1]));
                            row++;
                        }
                        else
                        {
                            frecAcumulada.Add(row == 0 ? 0 : frecAcumulada[i - 1]);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                log.Info("Error at getting opinions : \n" + e.Message);
            }

            return frecAcumulada;
        }

        public static void InsertOpinionLimitsInDB(double firstLimit, double secondLimit, double thirdLimit)
        {
            string insertLimitMALA = @"UPDATE dbo.OpinionLimit SET limitSup = @limitSup WHERE name = 'Mala'";
            string insertLimitNEUTRA = @"UPDATE dbo.OpinionLimit SET limitSup = @limitSup WHERE name = 'Neutra'";
            string insertLimitBUENA = @"UPDATE dbo.OpinionLimit SET limitSup = @limitSup WHERE name = 'Buena'";

            insertLimitMALA = insertLimitMALA.Replace("@limitSup", firstLimit.ToString());
            insertLimitNEUTRA = insertLimitNEUTRA.Replace("@limitSup", secondLimit.ToString());
            insertLimitBUENA = insertLimitBUENA.Replace("@limitSup", thirdLimit.ToString());

            try
            {
                GetDataTable(Query(insertLimitMALA));
                GetDataTable(Query(insertLimitNEUTRA));
                GetDataTable(Query(insertLimitBUENA));

                log.Info("Limits updated successfully");
            }
            catch (Exception e)
            {
                log.Error("Error at updating limits");
            }
        }

        #endregion

        #region Key phrases

        public static void ExtractKeyPhrasesFromNews()
        {
            log.Info("Opening conexion for text analitics ...");
            if (!OpenConection())
            {
                return;
            }

            List<MultiLanguageInput> documents = BuildKeyPhraseRequest();

            if (documents.Count > 0)
            {
                ITextAnalyticsAPI client = new TextAnalyticsAPI();
                client.AzureRegion = AzureRegions.Westus;
                client.SubscriptionKey = API_TEXT_ANALITICS_KEY;

                KeyPhraseBatchResult result = client.KeyPhrases(new MultiLanguageBatchInput(documents));

                int idNews, n = 0;
                foreach (var document in result.Documents)
                {
                    idNews = int.Parse(document.Id);

                    transaction = connection.BeginTransaction("Inserting key phrases");

                    if (InsertKeyPhrasesInDB(document.KeyPhrases))
                    {
                        var idsKeyPhrases = GetIdsKeyPhrases(document.KeyPhrases);

                        if (InsertNewsKeyPhraseInDB(idNews, idsKeyPhrases))
                        {
                            transaction.Commit();
                        }
                        else
                        {
                            transaction.Rollback();
                        }
                    }
                    else
                    {
                        transaction.Rollback();
                    }

                    n++;
                    if (((n % (MAX_NUMBER_KEY_PHRASE / 10)) == 0) || n == MAX_NUMBER_KEY_PHRASE)
                    {
                        log.Info(String.Format("Succeed at processing key phrases {0:0.00} %", (n * 100 / MAX_NUMBER_KEY_PHRASE)));
                    }
                }
            }

            CloseConnection();
        }

        public static List<MultiLanguageInput> BuildKeyPhraseRequest()
        {
            List<MultiLanguageInput> documents = new List<MultiLanguageInput>(MAX_NUMBER_KEY_PHRASE);

            StringBuilder queryDescNews = new StringBuilder(@"IF @MAX_NUMBER_KEY_PHRASE <= (
                                                            SELECT COUNT(N.id) FROM News.News AS N 
                                                            LEFT JOIN News.News_KeyPhrase AS NK ON N.id = NK.idNews
                                                            WHERE NK.idKeyPhrase IS NULL) 

	                                                            SELECT TOP @MAX_NUMBER_KEY_PHRASE N.id, N.description FROM News.News AS N 
	                                                            LEFT JOIN News.News_KeyPhrase AS NK ON N.id = NK.idNews
	                                                            WHERE NK.idKeyPhrase IS NULL");

            queryDescNews.Replace("@MAX_NUMBER_KEY_PHRASE", MAX_NUMBER_KEY_PHRASE.ToString());

            try
            {
                DataTable NewsDescriptions = GetDataTable(Query(queryDescNews.ToString()));

                string idNews, text;

                foreach (DataRow row in NewsDescriptions.Rows)
                {
                    idNews = row.ItemArray[0].ToString();
                    text = row.ItemArray[1].ToString();

                    documents.Add(new MultiLanguageInput(LANGUAGE_TEXT_ANALITICS, idNews, text));
                }

                log.Info("Extracting for key phrases succeed");
            }
            catch (Exception e)
            {
                log.Error("Error at searching news for key phrases extraction: \n" + queryDescNews + "\n" + e.Message);
            }

            return documents;
        }

        public static bool InsertKeyPhrasesInDB(IList<string> keyPhrases)
        {
            StringBuilder sb = new StringBuilder(@"INSERT INTO dbo.KeyPhrase (name)
			                                       SELECT * FROM (VALUES @values) as T(name)
			                                       EXCEPT
			                                       SELECT name FROM dbo.KeyPhrase");
            StringBuilder valuesSB = new StringBuilder();
            foreach (var kp in keyPhrases)
            {
                valuesSB.Append("('");
                valuesSB.Append(kp);
                valuesSB.Append("'),");
            }
            valuesSB.Remove(valuesSB.Length - 1, 1);

            sb.Replace("@values", valuesSB.ToString());

            try
            {
                DataTable idsKeyPhrases = GetDataTable(Query(sb.ToString()));
                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at inserting keyPhrases: \n" + sb.ToString() + "\n" + e.Message);
                return false;
            }
        }

        public static List<int> GetIdsKeyPhrases(IList<string> keyPhrases)
        {
            List<int> ids = new List<int>();
            StringBuilder sb = new StringBuilder(@"SELECT id FROM dbo.KeyPhrase WHERE name in (@keys)");

            StringBuilder values = new StringBuilder();
            foreach (var key in keyPhrases)
            {
                values.Append("'");
                values.Append(key);
                values.Append("',");
            }
            values.Remove(values.Length - 1, 1);

            sb.Replace("@keys", values.ToString());

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));

                if (result.Rows.Count > 0)
                {
                    foreach (DataRow row in result.Rows)
                    {
                        ids.Add(int.Parse(row[0].ToString()));
                    }

                    return ids;
                }
            }
            catch (Exception e)
            {
                log.Error("Error at searching keyPhrases: \n" + sb.ToString() + "\n" + e.Message);
            }
            return ids;
        }

        public static bool InsertNewsKeyPhraseInDB(int idNews, List<int> idsKeyPhrase)
        {
            if (idsKeyPhrase == null || idsKeyPhrase.Count == 0)
            {
                return false;
            }

            StringBuilder queryInsertKeyPhrases = new StringBuilder(@"INSERT INTO News.News_KeyPhrase (idNews, idKeyPhrase)
			                                                         VALUES @values ");
            StringBuilder sb = new StringBuilder(idsKeyPhrase.Count * 20);
            foreach (var idKey in idsKeyPhrase)
            {
                sb.Append("(");
                sb.Append(idNews);
                sb.Append(",");
                sb.Append(idKey);
                sb.Append("),");
            }
            sb.Remove(sb.Length - 1, 1);

            queryInsertKeyPhrases.Replace("@values", sb.ToString());

            try
            {
                DataTable idsKeyPhrases = GetDataTable(Query(queryInsertKeyPhrases.ToString()));

                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at inserting keyPhrases: \n" + queryInsertKeyPhrases + "\n" + e.Message);
                return false;
            }
        }

        #endregion

        #endregion

        #region videos

        public static void SearchVideos()
        {
            log.Info("Opening conexion for News Search...");
            if (!OpenConection())
            {
                return;
            }

            JObject json;
            int idTerm;
            int? idPublisher = null
                , idEncoding = null
                , idCreator = null
                , idVideo = null;
            string term;

            DataTable termsToSearch = GetTermsToSearch();

            if (termsToSearch != null)
            {
                foreach (DataRow row in termsToSearch.Rows)
                {
                    idTerm = Int32.Parse(row.ItemArray[0].ToString());
                    term = row.ItemArray[1].ToString();

                    log.Info("\n\n>>>>>>>>>>>>>>>>>>>>>Searching videos for '" + term + "' <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

                    json = JObject.Parse(BingVideoSearch(term));

                    foreach (var value in json["value"])
                    {
                        transaction = connection.BeginTransaction("Check Video");

                        idVideo = IsVideoInDB(value["contentUrl"]?.ToString());

                        if (idVideo != null)
                        {
                            if (InsertVideoTermTosearchInDB(idVideo.Value, idTerm))
                            {
                                transaction.Commit();
                                continue;
                            }
                            else
                            {
                                transaction.Rollback();
                                continue;
                            }
                        }
                        else
                        {
                            transaction.Commit();
                        }


                        if (idVideo == null)
                        {
                            transaction = connection.BeginTransaction("Inserting Video");

                            if (InsertPublisherVideo(value["publisher"], ref idPublisher) &&
                                InsertEncodingFormat(value["encodingFormat"], ref idEncoding) &&
                                InsertCreatorVideo(value["creator"], ref idCreator) &&
                                InsertVideoInDB(value, idPublisher, idEncoding, idCreator, ref idVideo) &&
                                InsertVideoTermTosearchInDB(idVideo.Value, idTerm))
                            {
                                log.Info("Video with all information inserted successfully: " + idVideo);
                                transaction.Commit();
                            }
                            else
                            {

                                transaction.Rollback();
                                continue;
                            }
                        }
                    }
                }
            }

            CloseConnection();
        }

        public static int? IsVideoInDB(string contentUrl)
        {
            if (string.IsNullOrWhiteSpace(contentUrl))
            {
                return null;
            }

            int? idVideo = null;
            string query = @"SELECT id from  Video.Video WHERE contentUrl = '" + contentUrl + "'";

            try
            {
                DataTable result = GetDataTable(Query(query));

                if (result.Rows.Count > 0)
                {
                    log.Info("Video already in DB: " + contentUrl);
                    idVideo = Int32.Parse(result.Rows[0].ItemArray[0].ToString());
                }
                else
                {
                    log.Info("Video does not exist in DB: " + contentUrl);
                }
            }
            catch (Exception e)
            {
                log.Error("Error at searching contentUrl: \n" + query + "\n" + e.Message);
            }

            return idVideo;
        }

        public static int? IsVideoTermToSearchInDB(int idVideo, int idTermToSearch)
        {
            int? idVideoTermToSearch = null;
            string query = @"SELECT id from dbo.Video_TermToSearch WHERE idVideo = " + idVideo + " AND idTermToSearch = " + idTermToSearch;

            try
            {
                DataTable result = GetDataTable(Query(query));

                if (result.Rows.Count > 0)
                {
                    idVideoTermToSearch = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                    log.Info("Video and Term already related in DB: " + idVideoTermToSearch);
                }
            }
            catch (Exception e)
            {
                log.Error("Error at searching Video_TermToSearch: \n" + query + "\n" + e.Message);
            }

            return idVideoTermToSearch;
        }

        public static bool InsertVideoTermTosearchInDB(int idVideo, int idTerm)
        {
            string query = @"IF NOT EXISTS(SELECT id FROM dbo.Video_TermToSearch WHERE idVideo = @idVideo AND idTermToSearch = @idTerm)
                            BEGIN
                            INSERT INTO [dbo].[Video_TermToSearch] (idVideo, idTermToSearch)
                            VALUES ('@idVideo','@idTerm') 
                            END";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@idVideo", idVideo.ToString());
            sb.Replace("@idTerm", idTerm.ToString());

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));

                log.Info("Succeded inserting Video_TermToSearch");
                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at inserting Video_TermToSearch: \n" + sb.ToString() + "\n" + e.Message);
                return false;
            }
        }

        public static bool InsertVideoInDB(JToken value, int? idPublisher, int? idEncoding, int? idCreator, ref int? idVideo)
        {
            string description = value["description"]?.ToString().Replace("'", "");
            string datePublished = value["datePublished"] != null ? DateTime.Parse(value["datePublished"].ToString()).ToString("yyyy-MM-dd hh:mm:ss") : null;
            string contentUrl = value["contentUrl"]?.ToString().Replace("'", ""); ;
            string hostPageUrl = value["hostPageUrl"]?.ToString().Replace("'", ""); ;
            string hostPageDisplayUrl = value["hostPageUrl"]?.ToString().Replace("'", ""); ;
            string width = value["width"]?.ToString();
            string height = value["height"]?.ToString();
            string duration = value["duration"] != null ? ParseIso8601(value["duration"].ToString()) : null;
            string embedHtml = value["embedHtml"]?.ToString().Replace("'", ""); ;
            string allowHttpsEmbed = value["allowHttpsEmbed"] != null ? (value["allowHttpsEmbed"].ToString() == "true" ? "1" : "0") : null;
            string viewCount = value["viewCount"]?.ToString();
            string thumbnailWidth = value["thumbnail"]?["width"]?.ToString();
            string thumbnailHeight = value["thumbnail"]?["height"]?.ToString();
            string videoId = value["videoId"]?.ToString();
            string allowMobileEmbed = value["allowMobileEmbed"] != null ? (value["allowMobileEmbed"].ToString() == "true" ? "1" : "0") : null;
            string name = value["name"]?.ToString().Replace("'", "");
            string thumbnailUrl = value["thumbnailUrl"]?.ToString().Replace("'", ""); ;
            string webSearchUrl = value["webSearchUrl"]?.ToString().Replace("'", ""); ;
            string motionThumbnailUrl = value["motionThumbnailUrl"]?.ToString().Replace("'", "");

            string query = @"INSERT INTO Video.Video (description,datePublished,idPublisher,idCreator,contentUrl,hostPageUrl
                                ,idEncodingFormat,hostPageDisplayUrl,width,height,duration,embedHtml,allowHttpsEmbed,viewCount,thumbnailWidth
                                ,thumbnailHeight,videoId,allowMobileEmbed,name,thumbnailUrl,webSearchUrl,motionThumbnailUrl)
                            VALUES
                        (@description,@datePublished,@idPublisher,@idCreator,@contentUrl,@hostPageUrl,@idEncodingFormat
                            ,@hostPageDisplayUrl,@width,@height,@duration,@embedHtml,@allowHttpsEmbed,@viewCount
                            ,@thumbnailWidth,@thumbnailHeight,@videoId,@allowMobileEmbed,@name,@thumbnailUrl,@webSearchUrl,@motionThumbnailUrl)
                            
                            SELECT @@IDENTITY AS 'ID' ";

            Func<string, string> AddSingleQuotes = s => s != null ? "'" + s + "'" : "NULL";
            Func<int?, string> AddSingleQuotesInt = s => s != null ? "'" + s + "'" : "NULL";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@description", AddSingleQuotes(description));
            sb.Replace("@datePublished", AddSingleQuotes(datePublished));
            sb.Replace("@idPublisher", AddSingleQuotesInt(idPublisher));
            sb.Replace("@idCreator", AddSingleQuotesInt(idCreator));
            sb.Replace("@contentUrl", AddSingleQuotes(contentUrl));
            sb.Replace("@hostPageUrl", AddSingleQuotes(hostPageUrl));
            sb.Replace("@idEncodingFormat", AddSingleQuotesInt(idEncoding));
            sb.Replace("@hostPageDisplayUrl", AddSingleQuotes(hostPageDisplayUrl));
            sb.Replace("@width", AddSingleQuotes(width));
            sb.Replace("@height", AddSingleQuotes(height));
            sb.Replace("@duration", AddSingleQuotes(duration));
            sb.Replace("@embedHtml", AddSingleQuotes(embedHtml));
            sb.Replace("@allowHttpsEmbed", AddSingleQuotes(allowHttpsEmbed));
            sb.Replace("@viewCount", AddSingleQuotes(viewCount));
            sb.Replace("@thumbnailWidth", AddSingleQuotes(thumbnailWidth));
            sb.Replace("@thumbnailHeight", AddSingleQuotes(thumbnailHeight));
            sb.Replace("@videoId", AddSingleQuotes(videoId));
            sb.Replace("@allowMobileEmbed", AddSingleQuotes(allowMobileEmbed));
            sb.Replace("@name", AddSingleQuotes(name));
            sb.Replace("@thumbnailUrl", AddSingleQuotes(thumbnailUrl));
            sb.Replace("@webSearchUrl", AddSingleQuotes(webSearchUrl));
            sb.Replace("@motionThumbnailUrl", AddSingleQuotes(motionThumbnailUrl));


            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                idVideo = int.Parse(result.Rows[0].ItemArray[0].ToString());

                log.Info("Succeded inserting Video: " + idVideo);

                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at inserting Video: \n" + sb.ToString() + "\n" + e.Message);

                idVideo = null;
                return false;
            }
        }

        public static string ParseIso8601(string valor)
        {
            string hrsRegex = @"\d+H";
            string minRegex = @"\d+M";
            string segRegex = @"\d+S";
            Regex regex;

            //string iso8601DurationRegex = @"^(P((?<Years>\d+)Y)?((?<Months>\d+)M)?((?<Days>\d+)D)?)(T((?<Hours>\d+)H)?((?<Minutes>\d+)M)?((?<Seconds>\d+((.)?(\d)?(\d)?))S)?)$";
            regex = new Regex(hrsRegex, RegexOptions.IgnoreCase);
            string hrs = regex.Match(valor).Value.Replace("H", "");

            regex = new Regex(minRegex, RegexOptions.IgnoreCase);
            string min = regex.Match(valor).Value.Replace("M", "");

            regex = new Regex(segRegex, RegexOptions.IgnoreCase);
            string seg = regex.Match(valor).Value.Replace("S", "");

            hrs = string.IsNullOrEmpty(hrs) ? "00" : hrs;
            min = string.IsNullOrEmpty(min) ? "00" : min;
            seg = string.IsNullOrEmpty(seg) ? "00" : seg;

            TimeSpan time = TimeSpan.Parse(hrs + ":" + min + ":" + seg);

            return time.ToString();
        }

        #region Publisher

        public static bool InsertPublisherVideo(JToken publisher, ref int? id)
        {
            if (publisher == null)
            {
                return false;
            }

            int? idPublisher;

            idPublisher = IsPublisherVideoInDB(publisher[0]["name"].ToString());

            if (idPublisher != null)
            {
                id = idPublisher;
            }
            else
            {
                idPublisher = InsertPublisherVideoInDB(publisher[0]["name"].ToString());
                if (idPublisher != null)
                {
                    id = idPublisher;
                }
            }

            return (id != null) ? true : false;
        }

        public static int? IsPublisherVideoInDB(string name)
        {
            int? idPublisher = null;
            string query = @"SELECT id from dbo.Publisher WHERE name = '" + name + "'";

            try
            {
                DataTable result = GetDataTable(Query(query));

                if (result.Rows.Count > 0)
                {
                    log.Info("PublisherVideo already in DB: " + name);
                    idPublisher = Int32.Parse(result.Rows[0].ItemArray[0].ToString());
                }
            }
            catch (Exception e)
            {
                log.Error("Error at searching publisher: \n" + query + "\n" + e.Message);
            }

            return idPublisher;
        }

        public static int? InsertPublisherVideoInDB(string name)
        {
            int? idPublisher = null;
            string query = @"INSERT INTO dbo.Publisher (name, idType) VALUES ('@name', NULL) 
                             SELECT @@IDENTITY AS 'ID' ";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@name", name);

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                idPublisher = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                log.Info("Succeded inserting PublisherVideo: " + idPublisher);
            }
            catch (Exception e)
            {
                log.Error("Error at inserting PublisherVideo: \n" + sb.ToString() + "\n" + e.Message);
            }

            return idPublisher;
        }

        #endregion

        #region EncodingFormat

        public static bool InsertEncodingFormat(JToken encoding, ref int? id)
        {
            if (encoding == null || string.IsNullOrWhiteSpace(encoding.ToString()))
            {
                return true;
            }

            int? idEncoding;

            idEncoding = IsEncodingFormatInDB(encoding.ToString());

            if (idEncoding != null)
            {
                id = idEncoding;
            }
            else
            {
                idEncoding = InsertEncodingformatInDB(encoding.ToString());
                if (idEncoding != null)
                {
                    id = idEncoding;
                }
            }

            return (id != null) ? true : false;
        }

        public static int? IsEncodingFormatInDB(string name)
        {
            int? idEncoding = null;
            string query = @"SELECT id from Video.EncodingFormat WHERE name = '" + name + "'";

            try
            {
                DataTable result = GetDataTable(Query(query));

                if (result.Rows.Count > 0)
                {
                    log.Info("EncodingFormat already in DB: " + name);
                    idEncoding = Int32.Parse(result.Rows[0].ItemArray[0].ToString());
                }
            }
            catch (Exception e)
            {
                log.Error("Error at searching EncodingFormat: \n" + query + "\n" + e.Message);
            }

            return idEncoding;
        }

        public static int? InsertEncodingformatInDB(string name)
        {
            int? idEncoding = null;
            string query = @"INSERT INTO Video.EncodingFormat (name) VALUES ('@name') 
                             SELECT @@IDENTITY AS 'ID' ";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@name", name);

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                idEncoding = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                log.Info("Succeded inserting EncodingFormat: " + idEncoding);
            }
            catch (Exception e)
            {
                log.Error("Error at inserting EncodingFormat: \n" + sb.ToString() + "\n" + e.Message);
            }

            return idEncoding;
        }

        #endregion

        #region Creator

        public static bool InsertCreatorVideo(JToken creator, ref int? id)
        {
            if (creator == null)
            {
                return true;
            }

            int? idCreator;

            idCreator = IsCreatorInDB(creator["name"].ToString());

            if (idCreator != null)
            {
                id = idCreator;
            }
            else
            {
                idCreator = InsertCreatorInDB(creator["name"].ToString());
                if (idCreator != null)
                {
                    id = idCreator;
                }
            }

            return (id != null) ? true : false;
        }

        public static int? IsCreatorInDB(string name)
        {
            int? idCreator = null;
            string query = @"SELECT id from Video.Creator WHERE name = '" + name + "'";

            try
            {
                DataTable result = GetDataTable(Query(query));

                if (result.Rows.Count > 0)
                {
                    log.Info("Creator already in DB: " + name);
                    idCreator = Int32.Parse(result.Rows[0].ItemArray[0].ToString());
                }
            }
            catch (Exception e)
            {
                log.Error("Error at searching Creator: \n" + query + "\n" + e.Message);
            }

            return idCreator;
        }

        public static int? InsertCreatorInDB(string name)
        {
            int? idCreator = null;
            string query = @"INSERT INTO Video.Creator (name) VALUES ('@name') 
                             SELECT @@IDENTITY AS 'ID' ";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@name", name);

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                idCreator = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                log.Info("Succeded inserting Creator: " + idCreator);
            }
            catch (Exception e)
            {
                log.Error("Error at inserting Creator: \n" + sb.ToString() + "\n" + e.Message);
            }

            return idCreator;
        }

        #endregion

        #endregion

        public static string JsonPrettyPrint(string json)
        {
            if (string.IsNullOrEmpty(json))
                return string.Empty;

            json = json.Replace(Environment.NewLine, "").Replace("\t", "");

            StringBuilder sb = new StringBuilder();
            bool quote = false;
            bool ignore = false;
            int offset = 0;
            int indentLength = 3;

            foreach (char ch in json)
            {
                switch (ch)
                {
                    case '"':
                        if (!ignore) quote = !quote;
                        break;
                    case '\'':
                        if (quote) ignore = !ignore;
                        break;
                }

                if (quote)
                    sb.Append(ch);
                else
                {
                    switch (ch)
                    {
                        case '{':
                        case '[':
                            sb.Append(ch);
                            sb.Append(Environment.NewLine);
                            sb.Append(new string(' ', ++offset * indentLength));
                            break;
                        case '}':
                        case ']':
                            sb.Append(Environment.NewLine);
                            sb.Append(new string(' ', --offset * indentLength));
                            sb.Append(ch);
                            break;
                        case ',':
                            sb.Append(ch);
                            sb.Append(Environment.NewLine);
                            sb.Append(new string(' ', offset * indentLength));
                            break;
                        case ':':
                            sb.Append(ch);
                            sb.Append(' ');
                            break;
                        default:
                            if (ch != ' ') sb.Append(ch);
                            break;
                    }
                }
            }

            return sb.ToString().Trim();
        }
    }
}
