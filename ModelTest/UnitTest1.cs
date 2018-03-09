using Microsoft.VisualStudio.TestTools.UnitTesting;
using Model;
using System;

namespace ModelTest
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        [Timeout(2000)]
        public void GetTermsToSearch()
        {
            using (DB_medIAEntities context = new DB_medIAEntities())
            {
                context.Database.Log = Console.Write;

                News news = new News();
                Mention mention = new Mention();
                News_Mention nm = new News_Mention();

                nm.Mention = mention;
                nm.News = news;

                news.News_Mention.Add(nm);

                context.News.Add(news);
                context.Mentions.Add(mention);
                context.News_Mention.Add(nm);

                context.SaveChanges();
            }
        }
    }
}
