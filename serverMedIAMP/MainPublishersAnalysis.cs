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

using Newtonsoft.Json.Linq;
using System.Text.RegularExpressions;
using System.Configuration;

using HtmlAgilityPack;

namespace serverMedIAMP
{
    public static class MainPublishersAnalysis
    {
        public static string API_SEARCH_KEY = ConfigurationManager.ConnectionStrings["API_SEARCH_KEY"].ConnectionString;
        public static string URI_API_NEWS_SEARCH_KEY = ConfigurationManager.ConnectionStrings["URI_API_NEWS_SEARCH_KEY"].ConnectionString;
        public static int MAX_NUMBER_NEWS = Int32.Parse(ConfigurationManager.ConnectionStrings["MAX_NUMBER_NEWS"].ConnectionString);
        public static string SQLDB_CONNECTION = ConfigurationManager.ConnectionStrings["SQLDB_CONNECTION_PROD"].ConnectionString;
        public static string API_TEXT_ANALITICS_KEY = ConfigurationManager.ConnectionStrings["API_TEXT_ANALITICS_KEY"].ConnectionString;
        public static string URI_API_TEXT_ANALITICS = ConfigurationManager.ConnectionStrings["URI_API_TEXT_ANALITICS"].ConnectionString;
        public static int MAX_NUMBER_OPINION = Int32.Parse(ConfigurationManager.ConnectionStrings["MAX_NUMBER_OPINION"].ConnectionString);
        public static string LANGUAGE_TEXT_ANALITICS = ConfigurationManager.ConnectionStrings["LANGUAGE_TEXT_ANALITICS"].ConnectionString;
        public static int MAX_NUMBER_KEY_PHRASE = Int32.Parse(ConfigurationManager.ConnectionStrings["MAX_NUMBER_KEY_PHRASE"].ConnectionString);
        public static string URI_API_VIDEOSEARCH_KEY = ConfigurationManager.ConnectionStrings["URI_API_VIDEOSEARCH_KEY"].ConnectionString;
        public static int MAX_NUMBER_VIDEOS = Int32.Parse(ConfigurationManager.ConnectionStrings["MAX_NUMBER_VIDEOS"].ConnectionString);
        public static string URI_API_SEARCH_KEY = ConfigurationManager.ConnectionStrings["URI_API_SEARCH_KEY"].ConnectionString;

        public static SqlConnection connection;
        public static SqlTransaction transaction;
        public static TraceWriter log;

        [FunctionName("MainPublishersAnalysis")]
        public static void Run([TimerTrigger("* * * * * *")]TimerInfo myTimer, TraceWriter _log)
        {
            log = _log;

            log.Info("Function app working >>");

            FixNews();

            //SearchMainPublishers();
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

        #region MainPublishers

        public static void FixNews()
        {
            log.Info("Opening conexion for main publishers news search...");
            if (!OpenConection())
            {
                return;
            }

            DataTable newsToFix = GetNewsToFix();
            int idNews;
            int idPublisher;
            string url;

            foreach (DataRow row in newsToFix.Rows)
            {
                idNews = int.Parse(row.ItemArray[0].ToString());
                idPublisher = int.Parse(row.ItemArray[1].ToString());
                url = row.ItemArray[2].ToString();

                transaction = connection.BeginTransaction("Update news");

                FixDateInDB(idNews, idPublisher, url);
                AddFullTextInDB(idNews, idPublisher, url);

                transaction.Commit();
            }

            CloseConnection();
        }

        public static DataTable GetNewsToFix()
        {
            DataTable result = null;

            try
            {
                result = GetDataTable(Query(@"SELECT N.id, NP.idPublisher, N.url FROM News.News N
		                                      INNER JOIN News.News_Publisher NP ON N.id = NP.idNews
		                                      WHERE YEAR(datePublished) = 1900"));
            }
            catch (Exception e)
            {
                log.Error("Error at getting News to fix : \n" + e.Message);
            }
            return result;
        }

        public static void SearchMainPublishers()
        {
            log.Info("Opening conexion for main publishers news search...");
            if (!OpenConection())
            {
                return;
            }

            var queries = GetSearchQueries();
            JObject json;
            JToken webPages;

            string querySearch;
            int? idNews;
            int idPublisher, idTerm;

            foreach (var query in queries)
            {
                idPublisher = query.Item1;
                idTerm = query.Item2;
                querySearch = query.Item3;

                log.Info(" >> " + querySearch);

                json = JObject.Parse(BingSearch(querySearch));
                webPages = json["webPages"];

                if (webPages != null)
                {
                    foreach (var value in webPages["value"])
                    {
                        transaction = connection.BeginTransaction("Check News");

                        idNews = IsNewsInDB(value["url"].ToString());

                        if (idNews != null)
                        {
                            if (InsertNewsTermTosearchInDB(idNews.Value, idTerm))
                            {
                                transaction.Commit();
                            }
                            else
                            {
                                transaction.Rollback();
                            }

                            // update date and text from previusly inserted news:

                            transaction = connection.BeginTransaction("Update news");

                            FixDateInDB(idNews.Value, idPublisher, value["url"].ToString());
                            AddFullTextInDB(idNews.Value, idPublisher, value["url"].ToString());

                            transaction.Commit();

                            continue;
                        }
                        else
                        {
                            transaction.Commit();
                        }

                        if (idNews == null)
                        {
                            transaction = connection.BeginTransaction("Insert News");

                            if (InsertNewsInDB(value, null, ref idNews, idPublisher) &&
                                InsertNewsTermTosearchInDB(idNews.Value, idTerm) &&
                                InsertNewsPublisherInDB(new List<int> { idPublisher }, idNews.Value))
                            {
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
                termsToSearch = GetDataTable(Query("SELECT id, term FROM TermToSearch WHERE status = 1"));
            }
            catch (Exception e)
            {
                log.Error("Error at getting terms to search : \n" + e.Message);
            }
            return termsToSearch;
        }

        public static DataTable GetURLMainPublishers()
        {
            DataTable mainPublishers = null;

            try
            {
                mainPublishers = GetDataTable(Query("SELECT idPublisher, url FROM MainPublisher WHERE status = 1"));
            }
            catch (Exception e)
            {
                log.Error("Error at getting main publishers \n" + e.Message);
            }
            return mainPublishers;
        }

        public static List<Tuple<int, int, string>> GetSearchQueries()
        {
            DataTable termsToSearch = GetTermsToSearch();
            DataTable mainPublishers = GetURLMainPublishers();

            int idPublisher, idTerm;
            string term, url;
            var queries = new List<Tuple<int, int, string>>(termsToSearch.Rows.Count * mainPublishers.Rows.Count);

            foreach (DataRow rowPub in mainPublishers.Rows)
            {
                idPublisher = int.Parse(rowPub.ItemArray[0].ToString());
                url = rowPub.ItemArray[1].ToString();

                foreach (DataRow rowTerm in termsToSearch.Rows)
                {
                    idTerm = int.Parse(rowTerm.ItemArray[0].ToString());
                    term = rowTerm.ItemArray[1].ToString();

                    queries.Add(new Tuple<int, int, string>(idPublisher, idTerm, term + " " + "(site:" + url + ")"));
                }
            }
            return queries;
        }

        #endregion

        #region Bing API Search

        public static string BingSearch(string searchQuery)
        {
            var uriQuery = URI_API_SEARCH_KEY + "?q=" + Uri.EscapeDataString(searchQuery) + "&count=" + MAX_NUMBER_NEWS + "&sortBy=Date";

            WebRequest request = HttpWebRequest.Create(uriQuery);
            request.Headers["Ocp-Apim-Subscription-Key"] = API_SEARCH_KEY;
            HttpWebResponse response = (HttpWebResponse)request.GetResponseAsync().Result;
            string json = new StreamReader(response.GetResponseStream()).ReadToEnd();

            return json;
        }

        #endregion

        #region News from main publishers

        public static void FixDateInDB(int idNews, int idPublisher, string url)
        {
            DateTime? dateInDB = GetDate(idNews);

            if ((dateInDB.HasValue && dateInDB.Value.Year == 1900) || !dateInDB.HasValue) // error de bing search API
            {
                DateTime? datePublished = GetDateFromUrl(url, idPublisher);

                if (datePublished != null)
                {
                    UpdateNewsDateInDB(datePublished.Value, idNews);
                }
            }
        }

        public static DateTime? GetDate(int idNews)
        {
            DateTime? date = null;

            try
            {
                DataTable result = GetDataTable(Query("SELECT datePublished FROM News.News WHERE id = " + idNews));

                if (result.Rows.Count > 0)
                {
                    date = DateTime.Parse(result.Rows[0].ItemArray[0].ToString());

                    return date;
                }
            }
            catch (Exception e)
            {
                log.Error("Error at getting date from idNews: " + idNews + "\n" + e.Message);
            }
            return date;
        }

        public static bool UpdateNewsDateInDB(DateTime datePublished, int idNews)
        {
            string query = @"UPDATE News.News SET datePublished = '@datePublished' where id = @idNews";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@datePublished", datePublished.ToString("yyyy-MM-dd hh:mm:ss"));
            sb.Replace("@idNews", idNews.ToString());

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at updating date idNews: \n" + idNews + "\n" + e.Message);
                return false;
            }
        }

        public static void AddFullTextInDB(int idNews, int idPublisher, string url)
        {
            string textInDB = GetText(idNews);

            if (string.IsNullOrEmpty(textInDB))
            {
                string text = GetTextFromUrl(url, idPublisher);

                if (text != null)
                {
                    UpdateNewsTextInDB(text, idNews);
                }
            }
        }

        public static string GetText(int idNews)
        {
            string text = null;

            try
            {
                DataTable result = GetDataTable(Query("SELECT text FROM News.News WHERE id = " + idNews));

                if (result.Rows.Count > 0)
                {
                    text = result.Rows[0].ItemArray[0].ToString();

                    return text;
                }
            }
            catch (Exception e)
            {
                log.Error("Error at getting text from idNews: " + idNews + "\n" + e.Message);
            }
            return text;
        }

        public static bool UpdateNewsTextInDB(string text, int idNews)
        {
            string query = @"UPDATE News.News SET text = '@text' where id = @idNews";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@text", text);
            sb.Replace("@idNews", idNews.ToString());

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at updating text idNews: \n" + idNews + "\n" + e.Message);
                return false;
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
                    idNews = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

                    return idNews;
                }
            }
            catch (Exception e)
            {
                log.Error("Error at searching url: \n" + query + "\n" + e.Message);
            }

            return idNews;
        }

        public static bool InsertNewsInDB(JToken value, int? idCategory, ref int? idNews, int idPublisher)
        {
            string datePublished = value["datePublished"] != null ? DateTime.Parse(value["datePublished"].ToString()).ToString("yyyy-MM-dd hh:mm:ss") : null;
            string name = value["name"].ToString().Replace("'", "");
            string url = value["url"].ToString();
            string description = value["description"] != null ?
                                 value["description"].ToString().Replace("'", "") :
                                 value["snippet"].ToString().Replace("'", "");

            if (datePublished == null)
            {
                DateTime? date = GetDateFromUrl(url, idPublisher);

                if (date.HasValue)
                {
                    datePublished = date.Value.ToString("yyyy-MM-dd hh:mm:ss");
                }
            }

            string text = GetTextFromUrl(url, idPublisher);

            string query = @"INSERT INTO News.News (idCategory,datePublished,name,url,description,text) VALUES 
                             (@idCategory, @datePublished, '@name', '@url', '@description', @text)
                             SELECT @@IDENTITY AS 'ID'";

            StringBuilder sb = new StringBuilder(query);
            sb.Replace("@idCategory", idCategory.HasValue ? "'" + idCategory.Value + "'" : "NULL");
            sb.Replace("@datePublished", datePublished != null ? "'" + datePublished + "'" : "NULL");
            sb.Replace("@name", name);
            sb.Replace("@url", url);
            sb.Replace("@description", description);
            sb.Replace("@text", !string.IsNullOrWhiteSpace(text) ? "'" + text + "'" : "NULL");

            try
            {
                DataTable result = GetDataTable(Query(sb.ToString()));
                idNews = Int32.Parse(result.Rows[0].ItemArray[0].ToString());

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
                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at inserting News_TermToSearch: \n" + sb.ToString() + "\n" + e.Message);
                return false;
            }
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
                return true;
            }
            catch (Exception e)
            {
                log.Error("Error at inserting News_Publisher: \n" + sb.ToString() + "\n" + e.Message);
                return false;
            }
        }

        #endregion

        #region web scraping

        public static DateTime? GetDateFromUrl(string url, int idPublisher)
        {
            DateTime? date = null;

            switch (idPublisher)
            {
                case 1532: //El universal 
                    date = GetDate("div", "class", "fechap", url);
                    break;
                case 1648: //Animal politico
                    date = GetDate("strong", "class", "entry-published", url);
                    break;
                case 2254: //Noticiero Televisa
                    date = fechaTelevisa(url);
                    break;
                case 2074: //milenio
                    date = GetDate("time", "itemprop", "datePublished", url);
                    break;
                default:
                    log.Info("Not found date from " + url);
                    break;
            }
            return date;
        }

        public static string GetTextFromUrl(string url, int idPublisher)
        {
            string text = null;

            switch (idPublisher)
            {
                case 1532: //El universal 
                    text = GetFullText("div", "class", "field field-name-body field-type-text-with-summary field-label-hidden", url);
                    break;
                case 1648: //Animal politico
                    text = GetFullText("section", "class", "entry-content", url);
                    break;
                case 2254: //Noticiero Televisa
                    text = GetFullText("div", "itemprop", "articleBody", url);
                    break;
                case 2074: //milenio
                    text = GetFullText("div", "itemprop", "articleBody", url);
                    break;
                default:
                    log.Info("Not found text from " + url);
                    break;
            }
            return text;
        }

        public static DateTime? GetDate(string nameTag, string attributeD, string idTitleF, string url)
        {
            var html = url;
            HtmlWeb web = new HtmlWeb();
            var htmlDoc = web.Load(html);
            var root = htmlDoc.DocumentNode;
            var fecha = root.Descendants().Where(n => n.GetAttributeValue(attributeD, "").Equals(idTitleF)).SingleOrDefault();

            if (fecha != null)
            {

                var date = fecha.InnerText;
                var date3 = date.ToString();

                DateTime fechatime;

                if (DateTime.TryParse(date3, out fechatime))
                {
                    return fechatime;
                }
                else
                {
                    try
                    {
                        string exp3 = "\\b((?<diasem>\\w(.*))\\s(?<diames>\\d{1,2})\\s(?<mes>\\w(.*))\\s(?<year>\\d{1,4})\\s(?<hora>\\w(.*)))";
                        var fe = Regex.Replace(date3, exp3, "${diames}/${mes}/${year} ${hora}", RegexOptions.None, TimeSpan.FromMilliseconds(150));
                        DateTime fechaf = Convert.ToDateTime(fe);

                        return fechaf;
                    }
                    catch (Exception e)
                    {
                        log.Error("No se pudo transformar el date " + fecha + "\n" + e.Message);
                        return null;
                    }
                }
            }
            return null;
        }

        public static DateTime fechaTelevisa(string url)
        {
            var html = url;
            HtmlWeb web = new HtmlWeb();
            var htmlDoc = web.Load(html);
            var root = htmlDoc.DocumentNode;

            var fecha1 = root.SelectSingleNode("//div[@class='single_post_info clearfix']");
            var date = fecha1.InnerText;

            var noticiaf = date.ToString();
            string[] words = noticiaf.Split('|');
            string fecha2 = words[3].Substring(0, 28);

            DateTime fechaf = Convert.ToDateTime(fecha2);
            return fechaf;
        }

        public static string GetFullText(string nombreTagCuerpo, string attributeN, string idCuerpoN, string url)
        {
            var html = url;
            HtmlWeb web = new HtmlWeb();
            var htmlDoc = web.Load(html);

            var root = htmlDoc.DocumentNode;
            var noticiaCuerpo = root.Descendants(nombreTagCuerpo).Where(n => n.GetAttributeValue(attributeN, "").Equals(idCuerpoN)).SingleOrDefault();

            if (noticiaCuerpo != null)
            {
                var newsB = WebUtility.HtmlDecode(noticiaCuerpo.InnerText);
                var noticiaB = newsB.ToString();
                return noticiaB;
            }
            return null;
        }

        #endregion
    }
}
